# SQL Layout

This folder contains the shared SQL abstraction layer plus one subdirectory per engine driver.

## What Lives Here

- `src/sql/include/dbprove/sql/` contains the public API used by the rest of the repo.
- `src/sql/connection_base.cpp` and `src/sql/connection_factory.cpp` hold the shared connection helpers and driver dispatch.
- `src/sql/engine.cpp` and `src/sql/include/dbprove/sql/engine.h` define engine names, aliases, defaults, and credential parsing.
- `src/sql/explain/` contains the canonical plan node model shared across engines.
- `src/sql/<engine>/` contains each engine-specific connection, result, row, and optional explain implementation.
- `src/sql/test/` contains smoke tests and local fixture wiring for the drivers that are exercised in CI or local development.

## Drivers In Practice

The source tree contains a mix of production-wired drivers and templates/prototypes.

Currently wired through `Engine` and `ConnectionFactory`:

- `mariadb`
- `postgresql`
- `sqlite`
- `mssql`
- `duckdb`
- `utopia`
- `databricks`
- `clickhouse`
- `yellowbrick`
- `trino`

Present in the tree but not fully wired into the runtime factory path:

- `snowflake`
- `boilerplate`

That distinction matters when adding a new driver: a directory and CMake target are not enough on their own. The engine must also be registered in `Engine` and `ConnectionFactory`.

## Session Bootstrap

For execution-plan work in a new session, load context in this order:

1. `ai-rules.md` at the repo root for contributor and AI-agent coding preferences.
2. `src/sql/explain/README.md` for canonical node semantics and shared rendering rules.
3. `src/sql/<engine>/README.md` for engine-specific explain source format and mapping quirks.
4. `src/sql/<engine>/explain.cpp` for actual parser and post-processing implementation.

## Key Locations

- `src/sql/explain/` contains shared plan node types and rendering behavior used by all engines.
- `src/sql/<engine>/explain.cpp` maps each engine plan format into canonical nodes.
- `src/sql/<engine>/README.md` should capture engine-specific setup, authentication, explain sources, and known limitations.
- `src/sql/test/explain.cpp` runs cross-engine explain smoke tests.
- `src/sql/test/fixture.cpp` controls which local engines participate in connectivity smoke tests.
- `src/sql/<engine>/tune/` contains dataset-specific tuning SQL.
- `docker/README.md` documents the local database containers currently available for smoke testing and benchmarking.

## Adding A New Driver

The fastest path is to copy `src/sql/boilerplate/` into a new `src/sql/<engine>/` directory and then wire the engine into the shared registration points.

### Required Code Changes

1. Add the new driver directory under `src/sql/<engine>/`.
2. Add a `CMakeLists.txt` in that directory and register it from `src/sql/CMakeLists.txt`.
3. Extend `Engine::Type` in `src/sql/include/dbprove/sql/engine.h`.
4. Extend `src/sql/engine.cpp`:
   - add string aliases in `Engine(const std::string_view name)`
   - add `defaultHost`, `defaultPort`, `defaultDatabase`, `defaultUsername`, `defaultPassword`, or `defaultToken` behavior as needed
   - add `parseCredentials`, `name`, and `internalName` entries
5. Include the new connection header in `src/sql/connection_factory.cpp` and add a `case` that constructs the driver.
6. If the driver should be linked into the main `dbprove::sql` target, add it to the parent `target_link_libraries` block in `src/sql/CMakeLists.txt`.
7. Add or update the engine README in `src/sql/<engine>/README.md`.

### Typical Driver Directory Shape

Most drivers follow this structure:

- `connection.h` and `connection.cpp` implement the engine connection and statement execution.
- `result.h` and `result.cpp` implement result traversal and row materialization.
- `row.h` and `row.cpp` adapt row access onto the result implementation.
- `explain.cpp` is optional but expected for engines that support canonical plan parsing.
- `parsers.h` is optional for engine-specific type conversion helpers.
- `tune/` is optional and only needed when theorem datasets need engine-specific indexes, statistics, or other tuning objects.
- `README.md` should document setup, auth, explain behavior, artifacts, and known limitations.

### Constructor Pattern

`ConnectionFactory` usually constructs drivers with:

- the engine-specific credential type
- the `Engine`
- an optional `artifacts_path`

Examples in the tree:

- `postgresql::Connection(const CredentialPassword&, const Engine&, std::optional<std::string>)`
- `duckdb::Connection(const CredentialFile&, const Engine&, std::optional<std::string>)`
- `databricks::Connection(const CredentialAccessToken&, const Engine&, std::optional<std::string>)`

If a new driver needs a different credential shape, the changes usually belong in both `Engine::parseCredentials(...)` and `ConnectionFactory::create()`.

### CMake Conventions

Current naming conventions are:

- implementation target: `dbprove_sql_<engine>`
- alias target: `dbprove::sql::<engine>`

Most drivers link:

- `dbprove::sql::core`
- `dbprove::sql::driver`

Engine-specific external libraries are linked from the driver-local `CMakeLists.txt`.

## Testing A New Driver

There are two separate questions:

1. Can the driver compile and link?
2. Can the driver connect to a real engine and return canonical results?

Useful places to wire verification:

- `src/sql/test/fixture.cpp` for local connectivity smoke tests.
- `src/sql/test/connection.cpp` for basic execute/fetch/fetchRow/fetchScalar coverage.
- `src/sql/test/explain.cpp` for canonical explain parsing coverage.
- `docker/README.md` and `docker/` if the engine should be easy to run locally.

Local smoke tests currently focus on the engines listed in `src/sql/test/fixture.cpp`, so a new driver is not automatically covered until it is added there.

## Explain Artifacts

Drivers that implement `ConnectionBase::explain(...)` can use `getArtefact(...)` and `storeArtefact(...)` from `ConnectionBase`.

Artifacts are stored under:

- `<artifacts_path>/<engine.internalName()>/<name>.<extension>`

That layout is shared across engines, so using `internalName()` consistently matters when registering a new driver.

## Tune Script Layout

This directory uses dataset-specific tune scripts per engine.

### Structure

Tune scripts live under each engine in a `tune/` folder:

- `src/sql/<engine>/tune/<dataset>.sql`
- `src/sql/<engine>/tune/<dataset>_drop.sql`

Examples:

- `src/sql/mssql/tune/tpch.sql`
- `src/sql/mssql/tune/tpch_drop.sql`
- `src/sql/postgresql/tune/tpch.sql`
- `src/sql/postgresql/tune/tpch_drop.sql`
- `src/sql/databricks/tune/tpch.sql`
- `src/sql/databricks/tune/tpch_drop.sql`

### Execution Behavior

When a theorem calls `ensureDataset("<dataset>")`, dbprove:

1. Ensures the dataset tables are present.
2. Looks for `src/sql/<engine>/tune/<dataset>.sql`.
3. Executes it if present.

`<dataset>_drop.sql` scripts are for safe teardown and are not auto-executed by `ensureDataset`.

### Script Requirements

- Scripts must be idempotent and safe to run repeatedly.
- Scripts should guard object creation with catalog checks or engine-specific `IF EXISTS` patterns.
- Drop scripts should be safe against partially existing datasets.
- Engine-specific syntax should be used where dependency ordering matters.
