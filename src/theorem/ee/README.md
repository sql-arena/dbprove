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
- around scales `4..5` an engine starts to spill, time out, or fail for memory reasons
- higher scales make that break obvious and repeatable

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
- `COUNT(join key)`
- `COUNT(selected payload columns)`

This keeps the result validation cheap and avoids paying large row materialization or client deserialization costs. The benchmark is supposed to measure join execution and aggregate completion, not output transfer.

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

## Storage Intent

For these tests we want engines to read parquet directly when possible:

- DuckDB uses parquet-backed views for this suite
- DataFusion reads parquet tables directly
- Trino should be compared as fairly as its configured catalog allows

The container setups also try to keep the parquet files in tmpfs so disk is not the dominant variable in runtime.

## Success Criteria

The suite is behaving as intended when:

- theorem output validates successfully
- runtimes are recorded for early scales
- later scales fail in an interpretable way
  - timeout
  - memory error
  - spill-related slowdown
- the resulting graph shows a visible cliff instead of a flat or random series
