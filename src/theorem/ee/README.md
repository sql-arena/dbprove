# EE Join Scale Tests

These `EE-JOIN-SCALE-*` theorems are meant to stress the execution engine, not the SQL parser or serializer.

## Goal

The benchmark is trying to create a join where:

- the probe side (`lineitem`) is kept at a fixed scaled size
- the build side (`orders`) grows with the theorem scale
- all engines can complete a few early scales fully in memory
- one or more engines then hit a clear memory wall as the build side gets larger

The useful shape for this suite is:

- scales `1..N` run successfully and stay in-memory
- a smaller-memory engine hits the wall first
- higher scales make that break obvious and repeatable

The current tuned ladder is:

- `1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16, 18, 20`

That gives us finer resolution around the early failure region without
requiring twenty separate timed points.

## Why This Is Synthetic

TPC-H `orders` and `lineitem` are not large enough on their own to expose the memory behavior we want, so the theorem scales both sides synthetically:

- `lineitem` is duplicated a fixed number of times
- `orders` is duplicated by the theorem scale
- join keys are shifted into disjoint key ranges so every scaled `orders` copy can still match the scaled `lineitem`

The join is still deterministic and the output is validated, but the purpose is to control join working-set size rather than model a natural workload exactly.

## Query Shape

The theorem intentionally returns exactly one row.

It uses:

- `COUNT(*)`
- one `SUM(...)` over values from both sides of the join
- `COUNT(selected payload columns)`

The mixed `SUM(...)` is deliberate. A pure `COUNT(...)` shape gave Trino too
much freedom to algebraically simplify or delay parts of the synthetic
expansion. The single-row output still keeps validation and client-side output
costs cheap.

## Materialized Inputs

The current harness no longer expresses the fixed `25x` `lineitem` duplication
and the scaled `orders` variants only as CTE cross joins.

Instead, `dbprove --prepare-ee-join-scale` materializes on the host:

- `run/materialized/join_scale/lineitem_25x/lineitem_25x.parquet`
- `run/materialized/join_scale/orders_scale_01/...`
- `run/materialized/join_scale/orders_scale_02/...`
- `run/materialized/join_scale/orders_scale_03/...`
- ...
- `run/materialized/join_scale/orders_scale_20/...`

Why this matters:

- DuckDB and DataFusion can read those parquet files directly.
- Trino can no longer legally hoist the fixed `25x` `lineitem` expansion above
  the expensive join, because it is already present in the scanned relation.
- The benchmark runner can stage the exact same materialized inputs into tmpfs
  for every engine.

The materialization is cached with a version marker. If the manifest and all
expected parquet files already exist, `dbprove --prepare-ee-join-scale` exits
without rebuilding them.

## Payload Tuning Strategy

The main tuning lever is the payload carried on the build side.

We want to increase or decrease the number of counted payload columns until the benchmark lands in the right range:

- too narrow: every engine succeeds too far up the scale ladder
- too wide: DataFusion or Trino fails already at scale `1`
- sweet spot: all engines survive a few scales, then one or more hit the wall around `4..5`

When tuning, prefer:

- plain source columns
- direct `COUNT(column)` aggregates

Avoid synthetic computed payload where possible. Simple counted source columns are easier to reason about and less likely to be distorted by optimizer rewrites.

## Trino Optimizer Findings

Trino has been the hardest engine to stress with this suite because it can
legally rewrite the SQL into shapes that are much cheaper than the theorem text
suggests.

What we have observed from saved `EXPLAIN` / `EXPLAIN ANALYZE` artifacts:

- Trino does perform real parquet-backed table scans.
- Trino does perform a real join.
- We have **not** yet observed a partial aggregate pushed below the join.
- The main optimizer tricks have instead been join decomposition and delayed
  cross products.

In particular, earlier query shapes allowed Trino to:

- join base `orders` to base `lineitem` first
- treat the bucket dimension as a tiny side relation
- apply the fixed `25x` `lineitem_multiplier` only after the main join

That means the expensive join could stay much smaller than intended, which made
the Trino runtime curve much flatter than DuckDB or DataFusion.

The current materialized-input harness is specifically meant to prevent that:

- `lineitem_25x` is already materialized on disk with `lineitem_replica_id`
- each `orders_scale_*` relation is already materialized on disk with `join_key`
  and `orders_replica_id`
- the measured query now joins those prebuilt relations instead of asking the
  optimizer to preserve the cross joins from SQL text alone

The current synthetic-key shape is:

- `join_key = orderkey * orders_scale + bucket`

This removes the tiny separate bucket join that Trino used in earlier
rewrites. Because `lineitem_25x` is now physically materialized, Trino can no
longer legally postpone the fixed `25x` probe-side expansion in the same way
it could when the multiplier only existed in SQL text.

### What To Watch For In Trino Plans

When debugging this suite on Trino, look for:

- `TableScan` on both `orders` and `lineitem`
- whether the main join is `PARTITIONED` or `REPLICATED`
- whether a top-level `CrossJoin` still multiplies the join output by `25`
- whether `Aggregate` stays above the join
- whether the build side is the widened `orders` relation or only a tiny side
  dimension

If a top-level `CrossJoin` still multiplies the result after the main join,
then Trino is still avoiding the intended fixed-size `25x` probe-side join cost.

## Storage Intent

For these tests we want engines to read parquet directly when possible:

- DuckDB uses parquet-backed views for this suite
- DataFusion reads parquet tables directly
- Trino reads the same parquet files through `Iceberg + Nessie`

The container setups stage those parquet files into tmpfs so disk is not the
dominant runtime variable.

## Current Boundary Picture

With the current materialized-input and `2GB` engine-budget setup, the rough
failure regions are:

- `DataFusion`: survives `1..5`, first real join-memory failure at `6`
- `Trino`: survives a few early scales, then mixes query-level OOM and
  container-level failure in the `5..10` range depending on the exact restart
  path
- `DuckDB`: stays smooth much longer and shows its main cliff around `14+`

Those boundaries are the result of the current harness, not a universal truth
about the engines. If the payload or memory envelope changes, the breakpoints
should be expected to move.

## Success Criteria

The suite is behaving as intended when:

- theorem output validates successfully
- runtimes are recorded for early scales
- later scales fail in an interpretable way
  - timeout
  - memory error
  - spill-related slowdown
- the resulting graph shows a visible cliff instead of a flat or random series
