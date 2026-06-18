# DuckDB AWS Iceberg

This image is the AWS-oriented DuckDB container for parquet-backed and
Iceberg-adjacent runs.

## Model

- The runtime image is based on `Ubuntu 22.04`.
- An EC2 instance mounts ephemeral NVMe storage into the container at `/mnt/nvme`.
- The container includes a prebuilt `dbprove` binary at `/opt/dbprove/bin/dbprove`.
- `dbprove` is expected to be invoked interactively over a terminal session inside the container.
- Any S3 download, parquet staging, or Iceberg registration work should be performed explicitly through `dbprove` or operator commands.
- The default working directory is `/mnt/nvme/duckdb`, which is intended to hold DuckDB database files and staged local artifacts.

## Optional environment

- `DUCKDB_NVME_ROOT`
  Defaults to `/mnt/nvme`.
- `DUCKDB_HOME`
  Defaults to `/mnt/nvme/duckdb`.
