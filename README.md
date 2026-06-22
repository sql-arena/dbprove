# dbprove
[![Build Status](https://travis-ci.org/DBProve/dbprove.svg?branch=master)](https://travis-ci.org/DBProve/dbprove)
[![Coverage Status](https://coveralls.io/repos/github/DBProve/dbprove/badge.svg?branch=master)](https://coveralls.io/github/DBProve/dbprove?branch=master)
Tool to analyse, benchmark and prove the capabilities of a database engine

# Building `dbprove` from source

The goal of `dbprove` is to be a static, fully self-contained binary. 

Whenever possible, the libraries needed from these vendors will be statically linked into
`dbprove`.

Of course, that won't always work - because some database vendors don't even distribute
their driver source or even allow static linking (ex: Oracle and Teradata). For those 
cases, `dbprove` will dynamically try to load the library at startup.

## CMake Presets
The project uses CMake presets for configuration. To build on macOS with Apple Silicon (ARM64), use:
```bash
cmake --preset osx-arm-base
cmake --build out/build/osx-arm-base --target dbprove
```
The `osx-arm-base` preset is configured to build with `CMAKE_BUILD_TYPE: Debug` by default to ensure symbols are available for debugging.

## Contributing to Driver development and adding your own Database Driver
There is a lot of work involved with adding new drivers to `dbprove`. Each database
vendor has their own idiosyncrasies. If you are interested in contributing or hooking up your
own database - please shoot me a mail and I can help you get started.

Since `dbprove` is an Apache licensed product, it means that static linking with
GPL licensed, prebuild libraries causes copy-left contamination. Because of this, drivers
that are licensed on less restrictive terms will be preferred. When no other driver
exists - dynamic og runtime loading  is acceptable. It is important that `dbprove`
can always run with drivers that it has statically linked and that newly added drivers
do not cause it to fail on startup.

The concrete driver-extension checklist now lives in:

- `src/sql/README.md` for engine registration, CMake wiring, artifacts, and test hooks
- `src/sql/boilerplate/README.md` for the template driver layout

## Running `dbprove`

User-facing CLI documentation for running `dbprove` should live in this root README.
For local workflow conventions such as which directory to invoke `dbprove` from, see [ai-rules.md](ai-rules.md).

Common flags:

- `-e <engine>` selects the engine to run against.
- `-T <selector[,selector...]>` selects one or more theorems to run.
  A selector can be a theorem name, a tag, or a category such as `PLAN`.
- `--docker` starts and stops the managed local docker image for engines that support local containerized runs.
- `--variant <native|iceberg>` selects the storage layout to use with `--docker`.
- `--artefact-dir <path>` replays required plan artefacts from an existing directory instead of generating them live.
- `--data-bucket <uri>` overrides the default source bucket used for shared input data.
- `--download-dir <path>` overrides where downloaded table data is staged locally. By default this is `./table_data` under the directory where `dbprove` is invoked.
- `--publish <name>` publishes the proof results from `./proof/` to the `dbprove-results` repository. See [Publishing results](#publishing-results) below.

Docker credential contract:

- Docker-managed engine containers are expected to use the same username,
  password, database, and similar connection defaults that `dbprove` itself
  returns from `Engine::defaultUsername(...)`, `Engine::defaultPassword(...)`,
  and related helpers.
- That convention is intentional. `dbprove --docker` should work without
  callers needing to supply extra credentials just to match a local container.
- If a docker image changes its bootstrap credentials, the corresponding
  `Engine` defaults must be updated in lockstep.

Shared input data assumptions:

- The generator path now treats local staged table data as a dual-format cache: each ensured table is expected to have both a `*.csv` file and a sibling `*.parquet` file under `table_data/`.
- When the source bucket is used as the backing store, we assume the bucket layout can satisfy that dual-format expectation, either by already containing both formats or by containing enough source data for `dbprove` to materialize the missing parquet locally after the CSV is ensured.
- This lets engines choose between classic CSV bulk load and direct parquet mounting/registration without changing theorem code.

Docker storage variants:

- `native` uses the engine's native local storage format.
- `iceberg` uses the object-store style layout used for parquet and Iceberg-oriented workflows.
- If `--variant` is omitted, `dbprove` uses the engine default.
- Some theorem suites require a specific storage variant. In those cases, `dbprove` enforces the theorem requirement and rejects conflicting `--variant` values.

Current local engine defaults:

- PostgreSQL, SQL Server, and ClickHouse default to `native`.
- Trino and DataFusion default to `iceberg`.

Docker lifecycle assumptions:

- `dbprove --docker` starts by killing every container it manages. This cleanup is name-based and includes the local Iceberg sidecar.
- If the selected docker storage variant is `iceberg`, `dbprove` starts the local `iceberg-catalog` sidecar first and waits for its health endpoint to return success before continuing.
- After that, `dbprove` starts exactly one engine-specific managed container and then runs the selected theorems.
- When the run ends, `dbprove` shuts down the managed docker project cleanly.
- There is no in-process recovery path. If cleanup, sidecar startup, sidecar health, engine startup, engine readiness, or theorem execution fails, `dbprove` exits with an error describing the failing step.

Examples:

```bash
cd run
../out/build/osx-arm-base/src/dbprove/dbprove -e duckdb -T TPCH-Q01
../out/build/osx-arm-base/src/dbprove/dbprove -e duckdb -T PLAN
../out/build/osx-arm-base/src/dbprove/dbprove -e postgresql -T CLI-1 --docker
../out/build/osx-arm-base/src/dbprove/dbprove -e trino -T EE-JOIN-SCALE-1 --docker --variant iceberg
../out/build/osx-arm-base/src/dbprove/dbprove -e mssql -T TPCH-Q01 --artefact-dir ./proof/SQL\\ Server/2022/artefacts
```

### Databricks Support
Databricks connectivity relies on a browser-based authentication flow for some features (like plan dumping). 

#### Databricks Authentication
Before running Databricks-related commands that require a browser session, run the authentication script:
```bash
./scripts/authenticate_databricks.sh
```
This script will open a browser window using Playwright. Complete the login and 2FA process, then close the browser. Your session will be saved in a local profile (`~/.databricks-playwright-profile`) and reused by `dbprove`.

#### Plan Artifacts
To make analysis and debugging easier, `dbprove` writes artifacts (scraped JSON, raw EXPLAIN output and other engine specific data) into the default proof output tree under `artefacts`. 
To replay from an existing artifact directory instead of running live queries, use `--artefact-dir <path>`.

```bash
dbprove -e Databricks ... --artefact-dir ./my_artifacts
```
When this flag is used, `dbprove` will check the specified directory for cached files (named `databricks_<hash>.json` and `databricks_<hash>.raw_explain`). It will then run without needing the engine to be up.
If a required artifact is missing, the run fails.


## Publishing results

After a successful run, `dbprove` writes proof result JSON files under `./proof/<engine>/<version>/` relative to the working directory. The `--publish <name>` flag copies those results into the `dbprove-results` repository so they can be reviewed and shared.

`dbprove` searches upward from the current working directory to locate a directory named `dbprove-results` and verifies that it is a git repository before proceeding. The `<name>` argument identifies the publisher and becomes a subdirectory under each engine-version path in the results repo:

```
dbprove-results/engine/<Engine>/<version>/<name>/
```

Usage (run from the same `run/` directory used for theorem runs):

```bash
cd run
../out/build/osx-arm-base/src/dbprove/dbprove --publish thomas
```

All JSON files from every engine and version found under `./proof/` are copied to the corresponding paths in the results repo. If an engine or version directory does not yet exist in the results repo it is created. A partial version-string match is attempted when an exact directory name is not found.

# Coding Guidelines
Detailed coding guidelines for contributors and AI agents are maintained in [ai-rules.md](ai-rules.md).

# Session Bootstrap (Explain Rendering)

For new sessions focused on execution-plan parsing/rendering, read these files in order:

1. `ai-rules.md` for contributor and AI-agent coding preferences.
2. `src/sql/clickhouse/README.md` for current ClickHouse-specific explain behavior and known edge cases.
3. `src/sql/explain/README.md` for canonical node model and shared rendering semantics across engines.
4. `src/sql/README.md` for SQL folder structure, tune scripts, and where engine-specific explain parsers live.
5. `src/sql/<engine>/README.md` for engine-specific plan source format and post-processing.

Core implementation files for ClickHouse explain rendering:

- `src/sql/clickhouse/explain.cpp` (plan extraction, parsing, post-processing, actual row reconstruction)
- `src/sql/clickhouse/dialect.{h,cpp}` (expression normalization and render-time rewrites)
- `src/sql/explain/*.h|*.cpp` (canonical nodes + SQL/tree rendering)

Typical local workflow:

```bash
cmake --preset osx-arm-base
cmake --build out/build/osx-arm-base --target dbprove test_connectivity
cd run
../out/build/osx-arm-base/src/sql/test/test_connectivity
```

# Thirdparty Rules

Thirdparty libraries are managed with `vcpkg`. In general, the goal is to keep third party dependencies to a minimum.

If you only need a single function from a massive library - please just make a header with that function instead of
pulling in the entire library.
