# Trino Connector

Connector for Trino (formerly PrestoSQL).

## Current Approach

This driver talks to Trino over the HTTP `/v1/statement` API using `libcurl`.

For the current benchmark harness, local theorem runs do **not** use Trino's
old built-in `tpch` connector anymore. The active join-scale path uses:

- Trino `481`
- the `Iceberg` connector
- a `Nessie` catalog
- parquet files staged into local tmpfs inside the Trino container

The driver still rewrites repo SQL into Trino's dialect where needed:

- `tpch.<table>` references are rewritten to `sf1.<table>` when the configured catalog is `tpch`
- `LEFT(x, n)` is rewritten to `SUBSTRING(x, 1, n)`
- bare ISO date strings like `'1998-10-01'` are rewritten to `DATE '1998-10-01'`

That keeps the theorem SQL mostly engine-agnostic from the caller's point of
view even though the backing catalog is now Iceberg/Nessie instead of the
built-in sample TPC-H source.

## Explain Support

`Connection::explain(...)` uses:

```sql
EXPLAIN (FORMAT JSON) <statement>
```

Trino returns a fragment-oriented JSON plan where `RemoteSource` nodes reference other fragments by id. The driver inlines those fragments and maps the resulting physical plan into the canonical `sql::explain` node model.

The explain parser uses a small balanced scanner rather than a pile of one-off regex rewrites. That parser is reused for:

- splitting top-level comma lists in plan metadata
- parsing `lhs := rhs` assignment details
- extracting Trino aggregate masks so they can become canonical filter nodes
- normalizing typed literals such as `integer '25'`, `boolean 'true'`, and `LikePattern '[%BRASS]'`
- normalizing unsafe Trino output symbols such as `null` before they become canonical SQL aliases
- stripping top-level type casts and catalog prefixes
- distinguishing real function-call syntax from infix operators before rewriting Trino helper functions such as `and(...)`
- relying on the shared expression cleaner to preserve infix operator keywords such as `BETWEEN` in executable subtree SQL
- honoring aliased `TableScan` outputs by inserting canonical projections when Trino exports renamed leaf symbols directly from scan nodes
- substituting assigned and child-output aliases back into executable expressions, including alias chains defined inside a single Trino node
- turning Trino mark-style `SemiJoin` outputs into canonical joins that still expose the boolean marker column needed by parent filters
- preserving `AssignUniqueId` outputs as explicit canonical projections so later grouping keys can reference the generated symbol
- replacing typed `Values` placeholders such as `null::double` with safe executable literals for actual-row gathering

This matters for actual-row gathering: the canonical plan needs executable subtree SQL, not just pretty printed planner text.

Operator families currently handled for TPC-H PLAN theorem coverage include:

- scans: `TableScan`, `ScanFilter`, `ScanProject`, `ScanFilterProject`
- joins: `InnerJoin`, `LeftJoin`, `RightJoin`, `CrossJoin`, `SemiJoin`
- grouping: `Aggregate`
- sorting and limits: `LocalMerge`, `PartialSort`, `TopN`, `TopNPartial`
- projection/filter wrappers: `Output`, `Project`, `FilterProject`
- exchange flow: `LocalExchange`, `RemoteSource`, `RemoteMerge`

Trino's JSON explain does not provide actual row counts in the same way as engines with `EXPLAIN ANALYZE` integrations, so canonical plans are built primarily from estimated row counts.
The Trino driver now reconstructs executable subtree SQL well enough to gather actual row counts across the full TPC-H `PLAN` workload in the containerized setup used by this repo.

### Running with Docker

To start a Trino instance for testing:
```bash
cd docker
docker compose up -d trino
```

The Web UI is available at `http://localhost:8080`.

For the join-scale benchmark, container startup now:

- copies the host materialized parquet tree into `/mnt/tpch-tmpfs/join_scale`
- bootstraps Iceberg tables through Nessie
- writes `/tmp/trino-bootstrap-ready` when the benchmark catalog is ready

The benchmark runner waits for both the HTTP health endpoint and that bootstrap
marker before treating Trino as ready.
