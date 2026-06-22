# Iceberg Catalog

This image provides the local Iceberg catalog sidecar used by `dbprove` when a
run uses both `--docker` and `--variant iceberg`.

The container runs Project Nessie, an internal single-node Trino instance, and
a small HTTP wrapper used by `dbprove` to register staged parquet files into
the shared Iceberg catalog.

## Shared Mount Convention

All local Iceberg-oriented containers mount the staged host data tree at:

- `/opt/dbprove/table_data`

These paths are the container-side contract for:

- the shared Iceberg catalog sidecar
- the Trino Iceberg container
- the DataFusion parquet-backed container

## Registration API

The sidecar exposes a small HTTP API on port `19130`.

- `GET /healthz`
- `POST /register-table`

`dbprove` uses that wrapper instead of invoking Trino commands directly. The
wrapper talks to the internal Trino instance over `127.0.0.1`, creates the
Iceberg table in the local Nessie-backed catalog, and runs `add_files(...)`
for the staged parquet inputs.
