Run dbprove theorems for a single engine and theorem selector.

Arguments: `$ARGUMENTS`

Parse the arguments as: `<engine> [<theorem-selector>]`

- `engine` is the `-e` value (e.g. `duckdb`, `postgresql`, `clickhouse`, `trino`, `datafusion`)
- `theorem-selector` is the `-T` value (e.g. `JOB`, `TPC-H`, `PLAN_JOIN_CHECK`). If omitted, run without `-T` (all theorems for that engine).

## Execution rules

- Always run from `/Users/thomaskejser/source/dbprove-agent1/run`
- Binary: `../out/build/osx-arm-base/src/dbprove/dbprove`
- **DuckDB**: no container needed — run directly.
- **Databricks**: no container. Always pass `--region us-east` (bucket is `s3://sql-arena-us-east`).
- **All other engines**: start the container first with docker compose, then run dbprove.
  - Compose project: `dbprove-managed`
  - Compose file: `/Users/thomaskejser/source/dbprove-agent1/docker/docker-compose.yml`
  - Service name maps: postgresql→postgresql, clickhouse→clickhouse, datafusion→datafusion-iceberg, trino→trino
  - Use: `docker compose -p dbprove-managed -f /Users/thomaskejser/source/dbprove-agent1/docker/docker-compose.yml up -d <service>`
  - Wait for container to be ready before running dbprove (check with `ps` or a short `docker compose exec` probe).
- Use `timeout=600000` on the dbprove Bash call.

## Reporting

After the run completes, report:
- Total theorems attempted
- Pass count and fail count
- Any failures with their error messages (grouped by error type if many)
- Whether failures are logic errors or resource limits (OOM, timeout)
