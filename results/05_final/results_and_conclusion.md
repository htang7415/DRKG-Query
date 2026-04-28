# Final Results Report

This report summarizes the packaged benchmark results in `results/05_final/`.

## Scope
- Baseline instances: PostgreSQL=`200`, DuckDB=`200`, Neo4j=`200`
- Join-order instances: `210`
- Reachability instances: `60`
- Matched engine instances: `200`
- Matching output-cardinality instances: `180`
- DuckDB matching output-cardinality instances: `184`

## Dataset
- Raw rows: `5874261`
- Unique edges: `5874229`
- Unique nodes: `97237`
- Unique relations: `107`
- Clean-up: dropped `29` empty-endpoint rows and `3` duplicate triples

## Templates
- Candidates enumerated: `135676`
- Templates selected: `5`
- Mining time: `1362.272` seconds

Selected templates:
- `P2`: `path` with `2` edges, grounded=`503732205`, anchors=`3978`
- `P3`: `path` with `3` edges, grounded=`140435650562`, anchors=`3974`
- `T1`: `tri` with `3` edges, grounded=`50701812`, anchors=`5070`
- `T2`: `tri` with `3` edges, grounded=`42863115`, anchors=`4556`
- `C4`: `cycle` with `4` edges, grounded=`113628`, anchors=`568`

## Engine Results
- Mean PostgreSQL/Neo4j runtime ratio: `0.603`
- Mean PostgreSQL/DuckDB runtime ratio: `3.729`
- C4 was added as a hard-query extension. PostgreSQL and DuckDB C4 rows use the standard baseline harness; Neo4j C4 rows use reduced measured timing without `PROFILE` to avoid the profile pass dominating the extension.

Per-template summary:
- `C4` / `hub` / `duck`: median=`12.468` ms, IQR=`11.438`-`13.326` ms, fully_ok=`20/20`, median output=`892.5`
- `C4` / `uniform` / `duck`: median=`9.821` ms, IQR=`9.125`-`10.32` ms, fully_ok=`20/20`, median output=`15.5`
- `P2` / `hub` / `duck`: median=`26.815` ms, IQR=`26.103`-`27.902` ms, fully_ok=`20/20`, median output=`586484`
- `P2` / `uniform` / `duck`: median=`20.186` ms, IQR=`17.375`-`22.841` ms, fully_ok=`20/20`, median output=`10774.5`
- `P3` / `hub` / `duck`: median=`1826.62` ms, IQR=`1245.331`-`2169.563` ms, fully_ok=`20/20`, median output=`211951006`
- `P3` / `uniform` / `duck`: median=`42.602` ms, IQR=`23.609`-`225.598` ms, fully_ok=`20/20`, median output=`487994`
- `T1` / `hub` / `duck`: median=`23.355` ms, IQR=`21.75`-`27.192` ms, fully_ok=`20/20`, median output=`45761`
- `T1` / `uniform` / `duck`: median=`16.086` ms, IQR=`14.527`-`16.799` ms, fully_ok=`20/20`, median output=`1606`
- `T2` / `hub` / `duck`: median=`23.922` ms, IQR=`23.192`-`28.514` ms, fully_ok=`20/20`, median output=`62113`
- `T2` / `uniform` / `duck`: median=`16.845` ms, IQR=`15.553`-`20.408` ms, fully_ok=`20/20`, median output=`2509.5`
- `C4` / `hub` / `neo`: median=`18.733` ms, IQR=`14.715`-`20.47` ms, fully_ok=`20/20`, median output=`892.5`
- `C4` / `uniform` / `neo`: median=`10.579` ms, IQR=`9.053`-`14.566` ms, fully_ok=`20/20`, median output=`15.5`
- `P2` / `hub` / `neo`: median=`400.827` ms, IQR=`348.081`-`515.425` ms, fully_ok=`0/20`, median output=`586484`, non_ok_rows=`20`
- `P2` / `uniform` / `neo`: median=`18.66` ms, IQR=`14.15`-`59.968` ms, fully_ok=`0/20`, median output=`10774.5`, non_ok_rows=`20`
- `P3` / `hub` / `neo`: median=`39399.389` ms, IQR=`39399.389`-`39399.389` ms, fully_ok=`0/20`, median output=`62212387`, non_ok_rows=`20`
- `P3` / `uniform` / `neo`: median=`213.775` ms, IQR=`54.249`-`3798.924` ms, fully_ok=`0/20`, median output=`304651`, non_ok_rows=`20`
- `T1` / `hub` / `neo`: median=`109.14` ms, IQR=`97.424`-`125.868` ms, fully_ok=`0/20`, median output=`45761`, non_ok_rows=`20`
- `T1` / `uniform` / `neo`: median=`28.538` ms, IQR=`20.177`-`40.991` ms, fully_ok=`0/20`, median output=`1606`, non_ok_rows=`20`
- `T2` / `hub` / `neo`: median=`103.488` ms, IQR=`82.714`-`117.422` ms, fully_ok=`0/20`, median output=`62113`, non_ok_rows=`20`
- `T2` / `uniform` / `neo`: median=`37.129` ms, IQR=`29.628`-`50.35` ms, fully_ok=`0/20`, median output=`2509.5`, non_ok_rows=`20`
- `C4` / `hub` / `pg`: median=`42.764` ms, IQR=`32.794`-`48.337` ms, fully_ok=`20/20`, median output=`892.5`
- `C4` / `uniform` / `pg`: median=`5.913` ms, IQR=`3.556`-`11.96` ms, fully_ok=`20/20`, median output=`15.5`
- `P2` / `hub` / `pg`: median=`167.873` ms, IQR=`130.046`-`227.053` ms, fully_ok=`20/20`, median output=`586484`
- `P2` / `uniform` / `pg`: median=`5.048` ms, IQR=`1.407`-`31.662` ms, fully_ok=`20/20`, median output=`10774.5`
- `P3` / `hub` / `pg`: median=`40905.42` ms, IQR=`34582.515`-`48022.457` ms, fully_ok=`5/20`, median output=`89618707`, non_ok_rows=`15`
- `P3` / `uniform` / `pg`: median=`106.576` ms, IQR=`31.402`-`1414.944` ms, fully_ok=`19/20`, median output=`304651`, non_ok_rows=`1`
- `T1` / `hub` / `pg`: median=`36.316` ms, IQR=`31.536`-`43.179` ms, fully_ok=`20/20`, median output=`45761`
- `T1` / `uniform` / `pg`: median=`3.383` ms, IQR=`1.745`-`5.843` ms, fully_ok=`20/20`, median output=`1606`
- `T2` / `hub` / `pg`: median=`40.327` ms, IQR=`27.949`-`41.735` ms, fully_ok=`20/20`, median output=`62113`
- `T2` / `uniform` / `pg`: median=`4.697` ms, IQR=`3.15`-`14.332` ms, fully_ok=`20/20`, median output=`2509.5`

## Reachability
Bounded reachability uses a small directional sample. Each row aggregates 5 anchors per (regime, depth) by reporting median anchor degree, median reachable count, and median runtime per engine.
- `hub` / depth `2` (5 anchors): anchor_deg_med=`23115`, reach_med=`45992`, pg=`726.589` ms, duck=`280.192` ms, neo=`337.748` ms
- `hub` / depth `3` (5 anchors): anchor_deg_med=`23115`, reach_med=`64843`, pg=`1860.033` ms, duck=`814.463` ms, neo=`595.506` ms
- `uniform` / depth `2` (5 anchors): anchor_deg_med=`2`, reach_med=`3`, pg=`2.083` ms, duck=`4.345` ms, neo=`17.152` ms
- `uniform` / depth `3` (5 anchors): anchor_deg_med=`2`, reach_med=`3`, pg=`0.831` ms, duck=`2.891` ms, neo=`11.459` ms

## Structure Summary
- `duck` / `hub` / `acyclic`: median=`1826.62` ms, fully_ok=`20/20`, median output=`211951006`, median AGM=`1628229415.5`
- `duck` / `hub` / `cyclic`: median=`22.504` ms, fully_ok=`60/60`, median output=`44310`, median AGM=`5986037.44`
- `duck` / `uniform` / `acyclic`: median=`42.602` ms, fully_ok=`20/20`, median output=`487994`, median AGM=`155857623`
- `duck` / `uniform` / `cyclic`: median=`15.024` ms, fully_ok=`60/60`, median output=`536.5`, median AGM=`3022102.299`
- `neo` / `hub` / `acyclic`: median=`39399.389` ms, fully_ok=`0/20`, median output=`62212387`, median AGM=`1628229415.5`
- `neo` / `hub` / `cyclic`: median=`92.403` ms, fully_ok=`20/60`, median output=`44310`, median AGM=`5986037.44`
- `neo` / `uniform` / `acyclic`: median=`213.775` ms, fully_ok=`0/20`, median output=`304651`, median AGM=`155857623`
- `neo` / `uniform` / `cyclic`: median=`24.371` ms, fully_ok=`20/60`, median output=`536.5`, median AGM=`3022102.299`
- `pg` / `hub` / `acyclic`: median=`40905.42` ms, fully_ok=`5/20`, median output=`89618707`, median work=`179825858`, median AGM=`1628229415.5`
- `pg` / `hub` / `cyclic`: median=`39.314` ms, fully_ok=`60/60`, median output=`44310`, median work=`147458`, median AGM=`5986037.44`
- `pg` / `uniform` / `acyclic`: median=`106.576` ms, fully_ok=`19/20`, median output=`304651`, median work=`625882`, median AGM=`155857623`
- `pg` / `uniform` / `cyclic`: median=`4.215` ms, fully_ok=`60/60`, median output=`536.5`, median work=`6117`, median AGM=`3022102.299`

## Join-Order Summary
- `P3` / `connected`: median=`2726.832` ms, fully_ok=`14/40`, median work=`16198502`
- `P3` / `cross`: median=`NA` ms, fully_ok=`0/20`
- `P3` / `default`: median=`2618.543` ms, fully_ok=`7/10`, median work=`16198502`
- `T1` / `connected`: median=`41.72` ms, fully_ok=`60/60`, median work=`149085`
- `T1` / `default`: median=`16.591` ms, fully_ok=`10/10`, median work=`90465.5`
- `T2` / `connected`: median=`37.303` ms, fully_ok=`60/60`, median work=`203475.5`
- `T2` / `default`: median=`18.495` ms, fully_ok=`10/10`, median work=`96872`

## Findings
- The benchmark now compares three execution models: Neo4j graph traversal, PostgreSQL row-store SQL, and DuckDB columnar SQL.
- Hub-anchored queries are consistently slower than uniform-random queries across the fixed-query workloads.
- The added 4-cycle `C4` directly tests a harder cyclic query shape beyond triangles.
- The acyclic 3-edge path `P3` is the hardest workload in the package; it is much slower and less stable than the triangle workloads despite being acyclic.
- Bounded reachability is a separate recursive/path-expansion workload: SQL engines expand a recursive frontier, while Neo4j variable-length Cypher enumerates paths before counting distinct endpoints.
- PostgreSQL join-order choice matters materially: connected/default orders for `P3` succeed on some bindings, while the cross-product-inducing class times out on all attempted `P3` bindings.
- Neo4j timing rows are present, but Neo4j profile-derived work metrics are unavailable in the saved package because the separate `PROFILE` pass failed runtime verification.

## Conclusion
The saved DRKG benchmark results support a practical conclusion rather than a purely structural one. Runtime is dominated by skew, anchor choice, output size, and intermediate expansion, not by the acyclic-versus-cyclic label alone. The expanded package adds DuckDB as a modern SQL engine and bounded reachability as a recursive workload, so the comparison now separates fixed typed joins from path-expansion behavior. The triangle queries remain comparatively stable, while the selected 3-edge path and hub reachability cases are the main stress cases.

## Files
- `final_tables/`: 4 story tables — selected templates, cross-engine summary, PG join-order summary, per-anchor reachability runtime.
- `final_figures/`: 6 story figures — template profile, engine runtime (with engine/pg ratio labels), structure runtime, AGM vs runtime, join-order effect, reachability runtime.
- `config_snapshot.yaml`: configuration used for the packaged run.
