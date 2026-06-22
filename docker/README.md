# Docker Infrastructure

This directory contains the containerized engine setups used by `dbprove`.

The shared local Iceberg catalog sidecar now lives under:

- `docker/iceberg/`

The directory shape now reserves a provider layer under each engine:

- `docker/<engine>/local/...` for local runs
- later `docker/<engine>/aws/...`, `docker/<engine>/azure/...`, and similar
- under each provider, `native/` is for engine-native storage layouts and
  `iceberg/` is for parquet-backed layouts that we expect to evolve toward
  Iceberg-backed runs

For remote provider containers, the convention is stricter:

- remote containers should use a common `Ubuntu 22.04` base unless there is a strong reason not to
- remote containers should ship with a prebuilt `dbprove` binary
- remote containers should copy that binary from a shared prebuilt image instead of rebuilding `dbprove` per engine image
- we plan to invoke `dbprove` through a terminal session inside the container
- provider-specific data staging such as S3 download should live in `dbprove`
  or in explicit operator commands, not in hidden image bootstrap logic

To support that, `docker/ubuntu/dbprove-build/` is the dedicated Ubuntu-based
builder image for producing `dbprove` binaries, and
`docker/ubuntu/dbprove-prebuilt/` is the shared artifact image that remote
engine images should copy from.

For local provider containers, the convention is different:

- local containers should not copy `dbprove` into the image by default
- local containers should stay focused on the engine runtime and its local data wiring
- if a local engine genuinely needs in-container `dbprove`, treat that as an explicit exception
- `DataFusion` is the main expected exception for now

One current caveat in the repo:

- the existing `duckdb/local/iceberg/` image still bakes in `dbprove`
- that image predates this clarified convention and should be treated as a legacy exception until we revisit it

For the current `EE-JOIN-SCALE-*` benchmark work, the important local image roots are:

- `duckdb/local/iceberg/`
- `datafusion/local/iceberg/`
- `trino/local/iceberg/`

The first AWS-oriented native Trino image root is:

- `trino/aws/native/`

The first AWS-oriented native PostgreSQL image root is:

- `postgresql/aws/native/`

The first AWS-oriented DuckDB iceberg image root is:

- `duckdb/aws/iceberg/`

The first AWS-oriented SQL Server native image root is:

- `mssql/aws/native/`

Those are the engines used by `scripts/run_scale.py`.

## Current Benchmarking Model

The join-scale benchmark tries to compare engines on similar footing:

- DuckDB runs inside a Linux container with a baked-in `dbprove` binary.
- DataFusion runs in a disposable container and reads staged materialized
  parquet from tmpfs.
- Trino runs as a compose service and reads staged materialized parquet
  through `Iceberg + Nessie`.

Before a benchmark run, `dbprove --prepare-ee-join-scale` materializes:

- one fixed `lineitem_25x` parquet dataset
- `orders_scale_*` parquet datasets for scales
  `1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16, 18, 20`

Those host-side parquet files live under the standard staged dataset tree:

- `run/table_data/scale/`

At benchmark runtime, each engine stages the host materialized tree into its
local tmpfs before executing any measured query.

The benchmark runner intentionally avoids keeping all engines up at once. We
want one engine active at a time to reduce resource contention and to free tmpfs
memory between runs.

## Benchmark Constraints

For the current `EE-JOIN-SCALE-*` work, we deliberately keep the engine
containers aligned:

- `DuckDB` container: `docker run --memory 6g`
- `DuckDB` engine: `SET memory_limit = '2GB'`
- `DataFusion` container: `6g`
- `DataFusion` engine: `datafusion-cli -m 2g`
- `Trino` container: `mem_limit: 6g`
- `Trino` engine:
  - `query.max-memory-per-node=2GB`
  - `query.max-memory=2GB`
  - `query.max-total-memory=2304MB`

And for storage:

- Iceberg-oriented containers share a common host-data mount path inside the
  container: `/opt/dbprove/table_data`
- DuckDB copies the staged `scale` dataset into `/mnt/tpch-tmpfs/scale`.
- DataFusion copies the staged `scale` dataset into `/mnt/tpch-tmpfs/scale`.
- Trino copies the staged `scale` dataset into `/mnt/tpch-tmpfs/scale`, then
  registers it into Iceberg tables in a Nessie catalog.
- The Trino tmpfs mount is created with `uid=1000,gid=1000,mode=755` so the
  non-root Trino process can recreate `/mnt/tpch-tmpfs/scale` and
  `/mnt/tpch-tmpfs/warehouse` reliably across container restarts.
- The tmpfs staging area is `4g`, because the full materialized join-scale
  parquet tree is about `1.9g`.

This means the benchmark is intended to compare execution engines with:

- a `6GB` container envelope
- a `4GB` tmpfs staging budget
- a `2GB` engine execution budget
- parquet-backed data
- tmpfs-resident source files
- a single active engine at a time

## Running Containers

General compose usage:

```bash
cd docker
docker compose up -d
docker compose down
```

To start only one compose-managed engine:

```bash
cd docker
docker compose up -d datafusion-iceberg
```

```bash
cd docker
docker compose up -d trino-iceberg
```

The benchmark runner does this automatically, so manual startup is mainly for
debugging.

## DuckDB

`duckdb/local/iceberg/` contains the current local DuckDB benchmark image.

Important points:

- The Dockerfile is multi-stage.
- It builds a Linux DuckDB-only `dbprove` binary inside the image build.
- It uses the prebuilt DuckDB SDK instead of compiling DuckDB from source.
- The final image reuses the build stage as its runtime base to avoid a second
  fragile `apt` path.
- At runtime, the entrypoint copies the materialized join-scale parquet tree
  from a host mount into tmpfs.
- The benchmark runner additionally constrains the container with
  `docker run --memory 6g`.
- Inside the engine, `dbprove` sets `memory_limit = '2GB'`.

This image is built directly by:

```bash
docker build --network=host -f docker/duckdb/local/iceberg/Dockerfile -t dbprove-duckdb-bench:latest .
```

The benchmark runner now calls that directly rather than going through an extra
wrapper script.

## DataFusion

`datafusion/local/iceberg/` contains the Apache DataFusion local container setup.

Important points:

- The image is built from source.
- Each invocation is launched through `docker run`, not a long-lived compose service.
- The DataFusion driver mounts `run/table_data` into the container.
- Startup copies the materialized parquet tree into tmpfs.
- `bootstrap.sql` registers `lineitem_25x` and the `orders_scale_*` parquet-backed tables.
- This container is used both for theorem runs and for plan/debug work.
- The DataFusion driver constrains the container with `--memory 6g`.
- Inside the engine, `datafusion-cli` is launched with `-m 2g`.

Examples:

```bash
cd docker
docker compose run --rm datafusion-iceberg
```

```bash
cd docker
docker compose run --rm -T datafusion-iceberg --format json -c "SELECT COUNT(*) FROM tpch.lineitem"
```

```bash
docker run --rm --entrypoint datafusion-plan-json dbprove-datafusion:latest \
  --sql "SELECT * FROM tpch.lineitem LIMIT 1"
```

```bash
cd docker
docker compose run --rm -T datafusion-iceberg -f /opt/datafusion/collect_statistics.sql
```

## Trino

`trino/local/iceberg/` contains the Trino local benchmark container.

Important points:

- It is managed as a compose service, but for benchmark fairness the runner
  restarts Trino between scales.
- It uses the `Iceberg` connector with a Nessie catalog sidecar (`iceberg-catalog`).
- Startup touches `/tmp/trino-bootstrap-ready` once Trino is ready to accept queries.
- Table registration is driven dynamically by `GeneratorState::registerIcebergTable`
  via the `iceberg-catalog` sidecar's `/register-table` endpoint; no static bootstrap
  SQL runs at container startup.
- The runner waits for `http://127.0.0.1:19130/healthz` (the iceberg-catalog sidecar)
  before loading datasets, which in turn blocks until Trino's internal Nessie instance
  is ready.
- Staged parquet files are read by Trino via the `local:///table_data/...` URI scheme,
  which resolves relative to `local.location=/opt/dbprove` in the catalog config, meaning
  `/opt/dbprove/table_data/...` inside the `iceberg-catalog` container.
- The runner waits for both:
  - `http://localhost:65432/v1/info`
  - `/tmp/trino-bootstrap-ready`
  before treating the external Trino service as query-ready.
- Docker-exposed engines all bind the same host port, `65432`. That is
  intentional: `dbprove --docker` expects to run one engine container at a
  time, and a port collision makes accidental multi-engine overlap fail loudly.
- The benchmark query itself is no longer used as Trino warmup. Warmup is now a
  lightweight `SELECT 1 ... LIMIT 1` against `lineitem_25x` so an early
  benchmark OOM does not get misclassified as a bootstrap problem.
- Trino theorem runs are executed one scale at a time with
  `dbprove --append-proof-csv` so the proof CSV survives those forced restarts.
- The entrypoint appends explicit Trino query memory limits on startup so the
  engine stays within the intended `2GB` execution budget even though the
  container has a `6GB` envelope for tmpfs staging and process overhead.
- Query timeouts are enforced by the `dbprove` Trino driver, not by the
  container itself.
- The container is started and stopped around the benchmark sweep so memory is
  released between engines.
- The compose service is capped with `mem_limit: 6g`.

`trino/aws/native/` contains the first AWS-oriented Trino native-storage container.

Important points:

- It expects an EC2 host mount of ephemeral NVMe storage at `/mnt/nvme`.
- It uses `Ubuntu 22.04` as the runtime base.
- It includes a prebuilt `dbprove` binary so the container can be used as a remote terminal-driven runtime.
- `dbprove` is expected to handle any S3 download or parquet staging work.
- The same NVMe mount stores the managed Trino warehouse in native local format.
- The container uses the Hive connector with a standalone metastore running in the same container and the local filesystem for table files.

`postgresql/aws/native/` contains the AWS-oriented PostgreSQL native-storage container.

Important points:

- It expects an EC2 host mount of ephemeral NVMe storage at `/mnt/nvme`.
- It uses `Ubuntu 22.04` as the runtime base.
- It includes a prebuilt `dbprove` binary so the container can be used as a remote terminal-driven runtime.
- `dbprove` is expected to handle any S3 download or staging work.
- PostgreSQL stores its cluster data under `/mnt/nvme/postgresql/data` by default.

`duckdb/aws/iceberg/` contains the AWS-oriented DuckDB container for parquet-backed and Iceberg-adjacent runs.

Important points:

- It expects an EC2 host mount of ephemeral NVMe storage at `/mnt/nvme`.
- It uses `Ubuntu 22.04` as the runtime base.
- It includes a prebuilt `dbprove` binary so the container can be used as a remote terminal-driven runtime.
- `dbprove` is expected to handle any S3 download, parquet staging, or Iceberg registration work.
- The default working directory is `/mnt/nvme/duckdb`, which is where local DuckDB database files and staged artifacts should live.

`mssql/aws/native/` contains the AWS-oriented SQL Server native-storage container.

Important points:

- It expects an EC2 host mount of ephemeral NVMe storage at `/mnt/nvme`.
- It uses `Ubuntu 22.04` as the runtime base.
- SQL Server on Linux is `amd64`-only in practice for this image, so build it with `docker buildx build --platform=linux/amd64` and pair it with an `amd64` `dbprove-prebuilt` image.
- It includes a prebuilt `dbprove` binary so the container can be used as a remote terminal-driven runtime.
- `dbprove` is expected to handle any S3 download or staging work.
- The image installs SQL Server 2022, `msodbcsql18`, and `mssql-tools18`.
- The default SQL Server credentials stay aligned with the repo defaults: user `sa`, password `YourStrong!Passw0rd`.
- New database files are directed into `/mnt/nvme/mssql/data`, while log, backup, and dump files are directed into neighboring NVMe-backed directories.

## Why The Materialized Inputs Matter

The in-query `lineitem_multiplier` and `orders_multiplier` shapes gave Trino too
much freedom to rewrite the benchmark:

- it could hoist the fixed `25x` `lineitem` expansion above the expensive join
- it could decompose the bucket dimension into a tiny side relation

Materializing:

- `lineitem_25x`
- `orders_scale_01` through `orders_scale_20` for the active tuned ladder

on the host removes that freedom. Each engine now scans the already-expanded
parquet relations directly.

## Why The Build Side Should Cross 2GB

The current join-scale theorem widens the `orders` build side using plain
counted source columns:

- `o_orderkey`
- `o_custkey`
- `o_orderstatus`
- `o_orderpriority`
- `o_clerk`
- `o_shippriority`
- `o_comment`

Using the SF1 parquet data, the variable-width columns are approximately:

- `o_orderstatus`: `1.0` bytes average
- `o_orderpriority`: `8.4` bytes average
- `o_clerk`: `15.0` bytes average
- `o_comment`: `48.5` bytes average

Together with the fixed-width integer fields, the raw projected `orders` row is
about `85` bytes before hash-table overhead, string/object overhead, nullability
bookkeeping, and allocator fragmentation.

At `1.5M` base `orders` rows, that gives a raw projected build-side size of
roughly:

- `~127 MB` per scale step

So:

- scale `4`: `~510 MB` raw projected payload
- scale `5`: `~637 MB` raw projected payload
- scale `6`: `~764 MB` raw projected payload

Since a hash-join build side is materially larger than the raw projected row
stream in memory, scales above `4..5` are expected to get close to or exceed a
`2GB` practical working set, depending on engine internals.

In practice the current harness lands roughly here:

- `DataFusion`: succeeds through `5`, then fails from `6`
- `Trino`: succeeds through a few early scales, then begins failing in the
  `5..10` range depending on whether the failure is query-level OOM or a harder
  connection/container event
- `DuckDB`: stays smooth much longer and shows its main cliff around `14+`

## Other Engines

These remain in the repository, but they are not part of the current join-scale
benchmark flow:

- `postgresql/local/native/`
- `clickhouse/local/native/`
- `mssql/local/native/`

## Notes For Future Updates

- Keep this README aligned with `scripts/run_scale.py`.
- If the DuckDB image flow changes again, update the “DuckDB” section here
  immediately.
- If a container stops being part of the benchmark path, say so here explicitly
  rather than letting old instructions linger.
