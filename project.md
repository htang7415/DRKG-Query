# Neo4j vs PostgreSQL on DRKG Conjunctive-Query Workloads

## 1. Objective

Compare Neo4j and PostgreSQL on matched conjunctive-query workloads derived from DRKG, and explain the results using CS 784 ideas: acyclic vs cyclic structure, AGM-style output bounds, join ordering, and skew.

## 2. Hypothesis

Acyclic templates should be more stable than cyclic templates under the same number of joins. Hub-anchored queries should increase intermediate expansion in both systems. Neo4j may do well on path-style traversals, while PostgreSQL may recover on some cyclic queries when join order is favorable. Yannakakis and worst-case optimal joins are used only as analytic reference points, not as claims about either system's implementation.

## 3. Systems and Reproducibility

Run both systems on one machine and report CPU, RAM, storage, OS, PostgreSQL version, and Neo4j 5.x Community Edition version.

Use:

- PostgreSQL with parallel query disabled
- Neo4j Community Edition with `CYPHER runtime=slotted` for every benchmark query
- The separate Neo4j `PROFILE` pass for each query instance must show `Runtime SLOTTED`
- Install PostgreSQL and Neo4j locally on the benchmark machine
- Create one dedicated local PostgreSQL role and one project database
- Use one authenticated local Neo4j account for the project instance
- Keep both systems bound to `localhost` unless remote access is explicitly needed
- Report and keep fixed across all runs the main memory/cache settings: PostgreSQL `shared_buffers`, `work_mem`, `effective_cache_size`, and any nondefault planner settings; Neo4j heap size and page-cache size
- Use either defaults or one manually tuned configuration per system chosen before benchmarking; do not retune per template, regime, binding, or engine pass
- The benchmark log must record the exact DBMS configuration, the restart procedure, and whether an OS page-cache flush was actually available on that machine

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

**Canonical data semantics:**

- `drkg.tsv` is the authoritative source of edge direction and endpoint types
- `node_type` is the substring before the first `::` in `node_id`
- Drop rows where either endpoint is of the form `<type>::` with an empty local identifier
- Deduplicate exact `(src_id, rel_type, dst_id)` triples on load; all benchmark queries use set semantics over this deduplicated graph
- Keep self-loops in storage, but benchmark templates enforce pairwise-distinct node variables, so self-loops cannot satisfy benchmark edges
- Use `relation_glossary.tsv` and `entity2src.tsv` only as metadata tables; do not infer edge direction from `relation_glossary.tsv`
- The `Connected entity-types` column in `relation_glossary.tsv` may disagree with the actual `(src_id, dst_id)` direction in `drkg.tsv` for some relations, so any endpoint-type checks must be derived from `drkg.tsv`

For theory and query specification, each relation type is treated logically as a filtered binary relation:

```
R_t(src_id, dst_id) := SELECT src_id, dst_id FROM edges WHERE rel_type = t
```

Here `t` is the raw relation ID from `drkg.tsv`; the benchmark harness maps that raw ID to the corresponding Neo4j relationship-type token.

## 5. Workload

Use paired SQL and Cypher templates with identical semantics.

**Acyclic templates:**

- 2-edge path
- 3-edge path
- 4-edge path

**Cyclic templates:**

- 2 triangle templates
- 1 four-cycle template, contingent on the threshold in Section 6

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

Mine candidate typed paths of lengths 2, 3, and 4, plus candidate typed triangles and typed 4-cycles, directly from the cleaned, deduplicated DRKG load defined in Section 4. A candidate template is identified by its ordered relation-type pattern together with its ordered endpoint-type pattern under the benchmark distinctness rules.

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

- A path or triangle candidate qualifies only if it has at least 100 grounded matches and at least 20 valid anchors after the Section 4 cleaning and deduplication rules
- A 4-cycle candidate qualifies only if it has at least 100 grounded matches and at least 20 valid anchors after the same rules

Selection rules:

- For each path length, select the highest-ranked qualifying path candidate as the benchmark path template of that length
- Select the two highest-ranked qualifying triangle candidates as the benchmark triangle templates
- If any 4-cycle qualifies, select the highest-ranked qualifying 4-cycle candidate as the benchmark 4-cycle template; otherwise the final workload is paths plus triangles only
- Ranking is by grounded match count descending, then valid-anchor count descending, then relation-type pattern under element-wise lexicographic tuple comparison, then endpoint-type pattern under the same element-wise tuple comparison

Freeze the final template set before benchmarking.

## 7. Metrics

**Core cross-system metrics:**

- Median wall-clock time over 10 plain measured executions
- IQR over the same 10 plain measured executions
- Output cardinality

**Execution-behavior metrics reported per engine, not as numerically identical cross-system quantities:**

- PostgreSQL buffer hits from `EXPLAIN (ANALYZE, BUFFERS)`
- PostgreSQL intermediate-work proxy: sum of `Actual Rows × Actual Loops` over plan nodes
- Neo4j total DB hits from `PROFILE`
- Neo4j intermediate-work proxy: sum of operator row counts from `PROFILE`

## 8. Measurement Protocol

A query instance is one fully instantiated run defined by:

- One template
- One regime
- One sampled anchor binding
- One engine
- And, for PostgreSQL forced-order experiments, one specific join order

All bindings are sampled once with a fixed recorded random seed before any benchmarking begins.

For each query instance:

1. Ensure the engine is in the fixed benchmark configuration for that system
2. If privileged OS page-cache flush is available, flush the OS page cache; otherwise skip this step and record that cold-start control is limited to DB restart only
3. Restart the DBMS
4. Run 1 untimed warmup execution of the plain query, without `EXPLAIN` or `PROFILE`
5. Apply a hard timeout of 60 seconds to the warmup and to each plain measured execution
6. Run 10 measured plain executions and report median and IQR
7. Run 1 separate untimed instrumented execution immediately afterward to collect plan metrics: `EXPLAIN (ANALYZE, BUFFERS)` for PostgreSQL and `PROFILE` with `Runtime SLOTTED` for Neo4j
8. Apply a hard timeout of 180 seconds to the instrumented execution
9. If the warmup or any plain measured run times out, exhausts memory, or fails with an execution error, abort the remaining runs for that query instance and log status, failure stage, failure type, timeout value, and completed measured-run count; record median, IQR, output cardinality, and instrumentation metrics as `NA` for that instance
10. If the warmup and all 10 plain measured runs succeed but the instrumented execution times out, exhausts memory, or fails with an execution error, keep median, IQR, and output cardinality from the successful plain runs, record instrumentation metrics as `NA`, and log the instrumented failure separately
11. Log timing and instrumentation metrics to CSV

The instrumented execution is excluded from wall-clock timing because it perturbs execution. The 10 measured executions all occur within the same post-restart instance, so the warmup is the only intended cache primer for that query instance.

**Binding counts:**

- Baseline benchmarking: 20 sampled bindings per template-regime, without replacement when enough valid anchors exist; otherwise use all valid anchors and report the shortfall
- Join-order study: 5 sampled bindings per template-regime, with the same rule

**Nominal experiment budget, if a 4-cycle qualifies and every template-regime has enough valid anchors:**

- Baseline: 480 query instances (6 templates × 2 regimes × 20 bindings × 2 engines)
- PostgreSQL join-order study: 710 query instances (660 forced-order instances + 50 default-plan comparator instances)

The 50 default-plan comparator instances use the same 5 bindings as the forced-order study, so the default-vs-forced comparison is matched.

## 9. Theory Lens

For each template:

- Define variables and relations
- Define the join hypergraph
- Classify it as acyclic or cyclic
- Compute the tightest AGM-style upper bound after the same relation filters and anchor binding by solving for the optimal fractional edge cover of the filtered join hypergraph; note separately that pairwise-distinctness predicates can only reduce the true result size
- Compare runtime and intermediate expansion against that bound

The most direct structure comparison is:

- 3-edge path vs triangle for the same join count
- 4-edge path vs 4-cycle if the 4-cycle qualifies

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
- 3-edge path: all 6 left-deep orders
- Each triangle: all 6 left-deep orders
- Each 4-relation template (4-edge path, and 4-cycle if it qualifies): all 24 left-deep orders

For each 4-relation template, classify the 24 orders into:

- **Connected-prefix orders**, where each newly joined relation shares at least one variable with the current intermediate result
- **Cross-product-inducing orders**, where some step joins a relation sharing no variable with the current intermediate result

These cross-product-inducing orders are intentional and quantify how badly poor join orders can blow up intermediate work.

## 11. Deliverables

- Deterministic preprocessing script that cleans and deduplicates `drkg.tsv` and emits the Neo4j relation-type mapping
- Neo4j ingestion and indexing scripts
- PostgreSQL ingestion and indexing scripts
- Paired SQL and Cypher templates
- Template-mining and selection script
- Parameter samplers
- Benchmark harness with CSV logging
- Final report linking results to query structure, AGM bounds, skew, and join-order sensitivity

## 12. Expected Contribution

A controlled comparison of Neo4j and PostgreSQL on matched DRKG conjunctive-query workloads, with results explained through query structure and execution behavior rather than raw timing alone.
