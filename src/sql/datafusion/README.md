# DataFusion Driver

## Container Setup

The local container lives under `docker/datafusion/local/iceberg/`.

Its job is to:

- build `datafusion-cli` from source
- build a small Rust `datafusion-plan-json` helper against the same DataFusion version
- mount staged host parquet files under `/opt/dbprove/table_data`
- prepare `scale` support tables in `/workspace/datafusion-bootstrap.sql`
- enable `datafusion.execution.collect_statistics`
- expose a helper that emits the physical execution plan as JSON

The container bootstrap is intentionally minimal now: base dataset tables are
registered by `dbprove` during theorem setup, not by the container init script.
Container startup and shutdown are owned by `src/dbprove/main.cpp` through the shared `dbprove --docker` flow. User-facing CLI usage is documented in the repo-root `README.md`.

## Runtime Contract

The driver under `src/sql/datafusion/` is container-backed and uses two execution paths:

- SQL execution and result fetching:
  - run the mounted `datafusion-cli` session inside the managed container
- Physical-plan JSON emission:
  - run the `datafusion-plan-json` helper inside the managed container
- Canonical plan parsing:
  - parse the Rust helper's protobuf-backed physical-plan JSON directly into the shared `sql::explain` nodes

This split is deliberate:

- native SQL `EXPLAIN FORMAT PGJSON` in DataFusion `53.1.0` is still logical-plan only in this setup
- DataFusion's Rust `datafusion-proto` crate already knows how to serialize physical plans
- using the helper keeps the JSON format version-matched with the engine and avoids regex parsing

## Singleton Session Contract

DataFusion is intentionally special-cased compared to the other `dbprove`
drivers.

- `ConnectionFactory::create()` still returns a fresh C++ wrapper object
- all DataFusion wrappers share one process-wide driver backend session
- `Connection::close()` is intentionally a no-op
- query execution is serialized inside the driver with a mutex

This exists because DataFusion state is session-oriented in this integration:

- schemas and external tables live in the CLI session
- `dbprove` frequently asks the factory for a new connection during dataset
  ensure, theorem execution, and tuning
- preserving the live DataFusion session across those calls is simpler and more
  reliable than replaying all state after every `factory.create()`

The mutex is deliberate. Multi-threaded theorem execution against DataFusion
does not run truly concurrently; the driver queues those requests onto the same
live session.

The shared bootstrap file at `/workspace/datafusion-bootstrap.sql` is still
kept up to date so fresh helper processes, such as `datafusion-plan-json`, can
reconstruct the same table state when needed.

## Current Limitations

- `bulkLoad(...)` is intentionally unimplemented because this driver targets the
  mounted parquet workflow
- canonical parsing currently focuses on the physical operators and expressions observed across the TPC-H workload
- the protobuf-backed JSON gives rich operator structure, but this pass does not use `EXPLAIN VERBOSE` at all
- connection semantics are intentionally unlike the other engines; callers must
  treat DataFusion as a shared serialized session rather than an isolated
  connection per `create()`
