# DataFusion Driver

## Container Setup

The local container lives under `docker/datafusion/`.

Its job is to:

- build `datafusion-cli` from source
- build a small Rust `datafusion-plan-json` helper against the same DataFusion version
- read the TPC-H SF1 Parquet files from a mount at `/opt/tpch-source/sf1/`
- register those files as external tables in a `tpch` schema
- enable `datafusion.execution.collect_statistics`
- expose a helper that emits the physical execution plan as JSON

The bootstrap is intentionally quiet now: table registration happens on every run, but the row-count warmup was moved to `docker/datafusion/collect_statistics.sql` so normal query output stays parseable.

## Runtime Contract

The driver under `src/sql/datafusion/` is container-backed and uses two execution paths:

- SQL execution and result fetching:
  - `docker run --rm dbprove-datafusion:latest -q --format json -b 1000000 -f /tmp/query.sql`
- Physical-plan JSON emission:
  - `docker run --rm --entrypoint datafusion-plan-json dbprove-datafusion:latest --sql-file /tmp/query.sql`
- Canonical plan parsing:
  - parse the Rust helper's protobuf-backed physical-plan JSON directly into the shared `sql::explain` nodes

This split is deliberate:

- native SQL `EXPLAIN FORMAT PGJSON` in DataFusion `53.1.0` is still logical-plan only in this setup
- DataFusion's Rust `datafusion-proto` crate already knows how to serialize physical plans
- using the helper keeps the JSON format version-matched with the engine and avoids regex parsing

## Validation Notes

Validated locally against the container:

- all 22 TPC-H queries run successfully against the rebuilt image
- `EXPLAIN FORMAT PGJSON` returns logical-plan JSON only
- the Rust helper emits structured physical-plan JSON for the same 22-query workload
- the `dbprove` theorem-backed DataFusion TPCH smoke test parses all 22 plans successfully

## Current Limitations

- `bulkLoad(...)` is intentionally unimplemented because this driver targets the pre-mounted TPCH container
- canonical parsing currently focuses on the physical operators and expressions observed across the TPC-H workload
- the protobuf-backed JSON gives rich operator structure, but this pass does not use `EXPLAIN VERBOSE` at all

## Future Cleanup

- if startup cost becomes noticeable, the next step is a small embedded DataFusion adapter instead of shelling into `docker run` per request

That would improve startup cost and give us a cleaner structured explain contract, but it is not required to get an initial integration working.

## How To Wire It Into `dbprove`

For the CLI-backed phase-1 driver, the wiring in this repo is straightforward:

1. Add `src/sql/datafusion/` by copying `src/sql/boilerplate/`.
2. Register `Engine::Type::DataFusion` in:
   - `src/sql/include/dbprove/sql/engine.h`
   - `src/sql/engine.cpp`
   - `src/sql/connection_factory.cpp`
   - `src/sql/CMakeLists.txt`
3. Model the connection layer on a process-backed transport:
   - use `popen()` or a small process helper
   - capture stdout and stderr separately if possible
   - treat non-zero exit codes as engine errors
4. Parse CLI JSON results into `ResultBase`.
5. Implement `bulkLoad(...)` as a no-op or `NotImplementedException`, because the container already mounts Parquet externally instead of ingesting CSV.
6. Implement `explain(...)` from the protobuf-backed JSON emitted by `datafusion-plan-json`.

## Alternative

The more reusable long-term option is either:

- a small HTTP adapter around embedded DataFusion, or
- Arrow Flight SQL on top of embedded DataFusion

Flight SQL is attractive if we expect multiple Flight SQL engines later.
An HTTP adapter is attractive if we want to stay close to the existing Trino-style integration pattern.

Both are larger investments than a CLI-backed first pass.
