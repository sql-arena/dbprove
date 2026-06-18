# Scripts

This directory holds project utility scripts. The most important one for the
current EE scale benchmarks is:

- `run_scale.py`
  - Runs the `EE-JOIN-SCALE-*`, `EE-SORT-SCALE-*`, and `EE-AGG-SCALE-*`
    theorem suites against `duckdb`, `datafusion`, and `trino`.
  - Regenerates the proof-derived markdown tables and:
    - `proof/join_scale_runtime.webp`
    - `proof/sort_scale_runtime.webp`
    - `proof/agg_scale_runtime.webp`
  - Uses `dbprove`'s own proof CSV output as the source of truth for runtime,
    stddev, tags, SQL text, and timeout/error reporting.

## Scale Runner

Typical usage:

```bash
python3 scripts/run_scale.py duckdb datafusion trino
```

Useful flags:

```bash
python3 scripts/run_scale.py duckdb datafusion trino \
  --suite all \
  --max-scale 20 \
  --query-timeout 60 \
  --timing-runs 2
```

```bash
python3 scripts/run_scale.py --report-only --suite all
```

```bash
python3 scripts/run_scale.py trino --suite join --scales 1,2,3,4,5,6,8
```

What it does:

- Calls `dbprove --prepare-ee-join-scale` once up front to materialize and
  cache the host-side parquet inputs under `run/materialized/join_scale/`.
- Cleans up latent benchmark containers before a new sweep.
- Builds the DuckDB benchmark image directly from `docker/duckdb/local/iceberg/Dockerfile`.
- Starts the needed benchmark containers one engine at a time.
- Runs the selected scale theorems through `dbprove`.
- Parses `proof/*/*/*_proof.csv`.
- Discovers graphable suites by looking for proof rows tagged with `scale`.
- Renders the report plots directly from the proof CSV rows rather than from
  separate temporary run CSVs or SQL snapshot directories.

Important behavior:

- DuckDB is intentionally run from a Linux container image so it matches the
  containerized benchmarking style of the other engines.
- DuckDB copies the materialized join-scale parquet tree into tmpfs and points
  `dbprove --parquet-dir` at `/mnt/tpch-tmpfs/join_scale`.
- DataFusion mounts the same host materialized tree into each disposable
  container and stages it into tmpfs before query execution.
- Trino stages the same host materialized tree into tmpfs, then registers the
  staged parquet files as Iceberg tables through Nessie.
- Trino is treated specially: the runner restarts Trino between scales and uses
  `dbprove --append-proof-csv` so each per-scale run appends into the same
  proof file instead of overwriting earlier scales.
- All scale theorems are tagged with:
  - `scale`
  - one of `JOIN`, `SORT`, or `AGG`
- The active tuned scale ladder is:
  `1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16, 18, 20`.

## Other Scripts

- `publish.sh`
  - Project publishing helper.
- `authenticate_databricks.sh`
  - Databricks auth helper.
- `dump_databricks_plan.mjs`
  - Dumps Databricks plan information.
- `choco-install.ps1`
  - Windows package bootstrap helper.
