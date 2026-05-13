# Docker Infrastructure

This directory contains Dockerfiles and configurations to start databases for benchmarking.

## Automation

You can use `docker compose` to manage the database engines.

To start all databases:
```bash
cd docker
docker compose up -d
```

To start a specific database (e.g., PostgreSQL) and ensure others are stopped:
```bash
cd docker
docker compose up -d --remove-orphans postgresql
```
Note: Always start only one engine at a time to avoid resource contention. The `--remove-orphans` flag or manual `docker compose down` followed by `docker compose up -d <engine>` is recommended.

To start an interactive Apache DataFusion CLI session with the TPC-H Parquet files pre-mounted:
```bash
cd docker
docker compose run --rm datafusion
```
This service is intentionally interactive rather than long-running: startup loads the `tpch` schema and registers the Parquet files.

To run a single query and emit machine-readable JSON:
```bash
cd docker
docker compose run --rm -T datafusion --format json -c "SELECT COUNT(*) FROM tpch.lineitem"
```

To collect a machine-readable physical-plan JSON tree:
```bash
docker run --rm --entrypoint datafusion-plan-json dbprove-datafusion:latest \
  --sql "SELECT * FROM tpch.lineitem LIMIT 1"
```

To refresh the statistics cache explicitly:
```bash
cd docker
docker compose run --rm -T datafusion -f /opt/datafusion/collect_statistics.sql
```

To stop all databases:
```bash
cd docker
docker compose down
```

## Engines

### PostgreSQL
- `postgresql/` - PostgreSQL configuration and setup (includes a benchmark-tuned config based on `postgres:latest`).

### ClickHouse
- `clickhouse/` - ClickHouse setup based on `clickhouse/clickhouse-server:26.1`.

### MSSQL (SQL Server)
- `mssql/` - SQL Server 2022 setup. Note: Default password is `YourStrong!Passw0rd`.

### Trino
- `trino/` - Trino setup based on `trinodb/trino:latest`.

### DataFusion
- `datafusion/` - Apache DataFusion CLI image built from source, preloaded with TPC-H Parquet data from `gs://sql-arena-data/tpch/sf1/`.
