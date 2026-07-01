Run dbprove theorems across all engines for a given theorem selector (or a default full suite).

Arguments: `$ARGUMENTS`

If an argument is given, use it as the `-T` theorem selector for all engines.
If no argument is given, run the default full suite: both `TPC-H` and `JOB` selectors.

## Engines to run (in this order)

1. `duckdb` — no container, run directly
1a. `databricks` — no container, always pass `--region us-east`
2. `postgresql`
3. `clickhouse`
4. `trino`
5. `datafusion`

**Run engines sequentially** — never in parallel. Each engine starts its container, runs, and the container stays up.

## Execution rules for each engine

- Always run from `/Users/thomaskejser/source/dbprove-agent1/run`
- Binary: `../out/build/osx-arm-base/src/dbprove/dbprove`
- **DuckDB**: no container, no `--docker` flag — run directly.
- **Databricks**: no container. Always pass `--region us-east`.
- **All other engines**: pass `--docker` and dbprove manages the container lifecycle itself. Do NOT manually run docker compose commands.
- Use `timeout=600000` on each dbprove Bash call.

When running the default full suite (no argument), run `TPC-H` first then `JOB` for each engine before moving to the next engine — i.e. complete one engine fully before starting the next.

## Reporting

After all engines finish, produce a summary table:

| Engine     | Selector | Passed | Failed | Notes |
|------------|----------|--------|--------|-------|
| duckdb     | TPC-H    | ...    | ...    |       |
| duckdb     | JOB      | ...    | ...    |       |
| ...        |          |        |        |       |

Group failures by error type (OOM, timeout, logic error) in the Notes column.
Flag any regressions compared to the known-good baseline in memory (project-plan-stability).
