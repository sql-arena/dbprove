# CedarDB connector

CedarDB is a PostgreSQL-compatible in-memory HTAP database (as of v2026-06-23). It exposes a libpq wire protocol endpoint on port 5432 and reports itself as `PostgreSQL 16.3 compatible CedarDB ...`. This means the connection layer can be shared with the PostgreSQL connector, but the EXPLAIN output format is entirely different.

## Connection

CedarDB uses the standard PostgreSQL wire protocol. The Docker image is `cedardb/cedardb:latest`.

Required startup environment variable: `CEDAR_PASSWORD` (not `POSTGRES_PASSWORD`). The superuser is created as `postgres`.

```
docker run -e CEDAR_PASSWORD=<password> -p 5432:5432 cedardb/cedardb:latest
```

## EXPLAIN

CedarDB has its own `EXPLAIN` dialect. Supported options are:

| Option | Support | Notes |
| :--- | :--- | :--- |
| `FORMAT JSON` | Yes | Primary structured format — use this for parsing |
| `FORMAT TEXT` | Yes | Human-readable with emoji decorations (default) |
| `FORMAT XML` | No | `unknown explain format "xml"` |
| `FORMAT YAML` | No | `unknown explain format "yaml"` |
| `ANALYZE` | Yes | Executes the query; adds `analyzePlanId`, `analyzePlanCardinality`, and `analyzePlanPipelines` |
| `VERBOSE` | Yes | Adds output columns and attribute lists to the text display |
| `COSTS` | No | `unknown explain option "costs"` |
| `BUFFERS` | No | `unknown explain option "buffers"` |
| `TIMING` | No | `unknown explain option "timing"` |

The recommended invocation for programmatic parsing:

```sql
EXPLAIN (ANALYZE, FORMAT JSON) <statement>
```

### JSON top-level structure

```json
{
  "plan": { ... },           // root operator node (see below)
  "ius": [ ... ],            // all intermediate-unit (IU) bindings in scope
  "output": [ ... ],         // result columns
  "type": "select",          // always "select" at the root
  "query": true,
  "permissions": { ... },    // table/schema ACL info
  "analyzePlanPipelines": [  // only present with ANALYZE
    {
      "start": 16,           // pipeline start time (µs from epoch)
      "stop": 269,
      "duration": 253,       // wall-clock µs
      "parallelism": "multi-threaded" | "single-threaded",
      "operators": [0, 1]    // operatorIds belonging to this pipeline
    }
  ]
}
```

### Operator node structure

Every node has at minimum:

| Field | Type | Notes |
| :--- | :--- | :--- |
| `operator` | string | Logical operator name (see table below) |
| `physicalOperator` | string | Physical implementation chosen by the optimizer |
| `cardinality` | number | Estimated row count |
| `operatorId` | number | Unique id within this plan; referenced by `analyzePlanPipelines` |
| `analyzePlanId` | number | Present with `ANALYZE` — maps this node to the runtime plan |
| `analyzePlanCardinality` | number | Actual row count (with `ANALYZE`) |

Children are embedded inline. Unary operators use an `"input"` field; binary operators use `"left"` and `"right"` (joins) or `"arguments"` (set operations).

## Operator catalogue

The following `operator` / `physicalOperator` pairs were observed on CedarDB v2026-06-23:

### tablescan

The universal scan node. The logical operator is always `tablescan`; the physical operator reveals the access strategy.

| physicalOperator | Meaning |
| :--- | :--- |
| `tablescan` | Full table scan (or filtered scan with no index) |
| `indexscan` | Index seek / range scan using a B-tree index |

Key fields:

```json
{
  "operator": "tablescan",
  "physicalOperator": "indexscan",
  "cardinality": 1,
  "tablename": "t1",
  "tableSize": 1000,
  "table": { "type": "table", "id": 0 },
  "attributes": [
    { "name": "a", "iu": "a" },
    { "name": "b", "iu": "b" }
  ],
  "restrictions": [
    {
      "attribute": 0,          // index into attributes array
      "mode": "=",             // see restriction modes below
      "value": { "expression": "const", "value": { "type": { "type": "int32" }, "value": 42 } },
      "estimatedSelectivity": 0.001,
      "collate": ""
    }
  ],
  "residuals": [],             // post-scan filter expressions not pushed into index
  "indexScan": {               // only present when physicalOperator == "indexscan"
    "prefixRestrictionIndexes": [0],
    "indexRef": 0,
    "partitionKeyBound": true
  },
  "indexname": "idx_t1_a"      // only present when physicalOperator == "indexscan"
}
```

**Restriction modes** observed:

| mode | Meaning |
| :--- | :--- |
| `=` | Equality |
| `>=` | Greater-or-equal (CedarDB normalises `>` to `>=` on integers) |
| `<=` | Less-or-equal |
| `[]` | Range / IN list — `value` is lower bound, `upper` is upper bound |
| `is` | IS NULL / IS NOT NULL (null value in `value.value` means IS NULL) |
| `isnotnull` | IS NOT NULL (also injected automatically by join null filtering) |
| `joinfilter` | Bloom-filter probe injected by the hash join into the probe-side scan |

Note: `>` and `<` are rewritten to `>=` / `<=` on integer literals (e.g. `a > 100` → `a >= 101`).

### join

All join variants share `"operator": "join"`.

| physicalOperator | Meaning |
| :--- | :--- |
| `hashjoin` | Hash join (most common for equality conditions) |
| `bnljoin` | Block nested-loop join (used for cross joins and cartesian products) |
| `indexnljoin` | Index nested-loop join (used when the inner side has a correlated index lookup, e.g. scalar subqueries) |

Key fields:

```json
{
  "operator": "join",
  "physicalOperator": "hashjoin",
  "left": { ... },             // build side
  "right": { ... },            // probe side
  "condition": {               // join predicate expression
    "expression": "compare",
    "left": { "expression": "iuref", "iu": "a" },
    "right": { "expression": "iuref", "iu": "x" },
    "direction": "=",
    "collate": ""
  },
  "type": "inner",             // join semantic type (see below)
  "filter": true               // true when a bloom filter was injected into probe-side scans
}
```

**Join semantic types** (`"type"` field):

| value | SQL equivalent |
| :--- | :--- |
| `inner` | INNER JOIN |
| `rightouter` | LEFT OUTER JOIN (CedarDB flips left→right internally) |
| `rightsemi` | IN / EXISTS semi-join (probe rows that match build rows) |
| `rightanti` | NOT IN / NOT EXISTS anti-join |
| `singletonjoin` | Scalar subquery join (the build side produces exactly one row) |

Note on orientation: CedarDB consistently places the smaller (build) relation as `left` and the larger (probe) relation as `right`. SQL `LEFT JOIN t1 ON ...` is represented as `"type":"rightouter"` because the nullable side ends up as the `left` child internally.

### sort

Used for ORDER BY, and also acts as the top-k / limit node when a LIMIT clause is attached.

| physicalOperator | Meaning |
| :--- | :--- |
| `sort` | Full sort (ORDER BY without LIMIT) |
| `simpletopk` | Top-k heap sort (ORDER BY … LIMIT n) |
| `limit` | Pure limit/offset without sort (seen as physicalOperator on sort when used with LIMIT+OFFSET only) |

Key fields:

```json
{
  "operator": "sort",
  "physicalOperator": "simpletopk",
  "order": [
    { "value": { "expression": "iuref", "iu": "a" }, "collate": "", "descending": true },
    { "value": { "expression": "iuref", "iu": "b" }, "collate": "" }
  ],
  "limit": { "expression": "const", "value": { "type": { "type": "int64" }, "value": 10 } },
  "offset": { "expression": "const", "value": { "type": { "type": "int64" }, "value": 5 } },
  "type": "simpletopk"
}
```

Ascending order has no `"descending"` key; descending adds `"descending": true`.

### groupby

Covers both GROUP BY aggregation and SELECT DISTINCT (DISTINCT is a group-by with no aggregates).

| physicalOperator | Meaning |
| :--- | :--- |
| `groupby` | Hash-based group-by (regular and distinct) |
| `ungroupedaggregation` | Aggregate over the entire relation (no GROUP BY keys) |

Key fields:

```json
{
  "operator": "groupby",
  "physicalOperator": "groupby",
  "key": [
    { "arg": 0, "iu": "a7", "collate": "" }
  ],
  "groupingmode": "regular",
  "aggregates": [
    { "op": "countstar", "iu": "count_star(*)" },
    { "op": "sum", "arg": 1, "collate": "", "iu": "sum()" }
  ],
  "orders": [],
  "groupingsets": [],
  "values": [ ... ],
  "considerspooling": false,
  "hasminspoolinglimit": true
}
```

Observed aggregate `op` values: `countstar`, `sum`, `count`, `avg`, `min`, `max`.

`groupingmode` observed values: `regular` (standard GROUP BY), possibly others for ROLLUP/CUBE.

HAVING is not a separate node: the predicate is pushed into a `select` wrapper node placed above the `groupby`.

### select

A filter / projection node. Used for:
- HAVING (wraps a `groupby`)
- Scalar expression filtering not pushed into a scan
- Scalar subquery result selection

```json
{
  "operator": "select",
  "physicalOperator": "select",
  "condition": { ... }
}
```

### setoperation

Covers UNION ALL, UNION, INTERSECT, and EXCEPT.

| physicalOperator | Meaning |
| :--- | :--- |
| `unionall` | UNION ALL |
| `setoperation` | UNION / INTERSECT / EXCEPT (deduplicating variants) |

Key fields:

```json
{
  "operator": "setoperation",
  "physicalOperator": "unionall",
  "operation": "unionall",   // or "union" | "intersect" | "except"
  "arguments": [
    {
      "input": { ... },
      "columns": [ { "expression": "iuref", "iu": "a" }, ... ]
    }
  ],
  "columns": [               // output IU bindings
    { "iu": "a13", "collate": "" }, ...
  ]
}
```

### window

Window functions (ROW_NUMBER, RANK, etc.).

```json
{
  "operator": "window",
  "physicalOperator": "window",
  "values": [ ... ],         // input expressions
  "partitions": [
    {
      "key": [ { "value": 0, "collate": "" } ],
      "orders": [
        {
          "order": [ { "value": 1, "collate": "" } ],
          "operations": [
            {
              "frame": {
                "range": "range",
                "exclude": "none",
                "start": { "mode": "unbounded" },
                "end": { "mode": "currentrow" }
              },
              "op": { "window": true, "op": "rownumber", "iu": "row_number" }
            }
          ]
        }
      ]
    }
  ]
}
```

## ANALYZE instrumentation

With `EXPLAIN (ANALYZE, FORMAT JSON)`:

- Each operator node gains:
  - `analyzePlanId`: integer cross-reference into the runtime plan
  - `analyzePlanCardinality`: actual rows produced

- The top-level `analyzePlanPipelines` array describes execution pipelines:
  - `start` / `stop` / `duration`: wall-clock timestamps in microseconds
  - `parallelism`: `"multi-threaded"` or `"single-threaded"`
  - `operators`: list of `operatorId` values belonging to this pipeline

CedarDB uses a pipeline-parallel (Morsel-driven) execution model. A single query may have multiple pipelines; the pipelines run sequentially when their parallelism is `single-threaded` and concurrently when `multi-threaded`.

There are **no per-operator timing fields** — timing is only available at pipeline granularity. There are also no I/O buffer metrics (CedarDB is in-memory; the text `ANALYZE` output shows `num IOs: 0, Fetched: 0 B` but these fields do not appear in JSON).

The text format `ANALYZE` output additionally shows:
- `Materialized: N KB` (hash table build memory)
- `Utilization: N %` (hash table fill ratio)
- `Time: N ms (N % ***)` (per-pipeline relative timing, shown as share of total)

## Expression language

Expressions appear in `condition`, `restrictions[].value`, `order[].value`, etc.

| `expression` | Meaning |
| :--- | :--- |
| `iuref` | Reference to an IU (intermediate unit = column binding) by name |
| `const` | Constant literal; `value.type.type` gives the SQL type |
| `parameter` | Prepared-statement parameter (`id` is the 0-based index) |
| `compare` | Binary comparison: `left`, `right`, `direction` (`=`, `<`, `>`, `<=`, `>=`, `<>`) |
| `cast` | Type cast: `input`, `type`, `semantic` (`implicit` or `explicit`) |

SQL types in `type.type`: `int32`, `int64`, `float8`, `text`, `bool`, `date`, `timestamp`, etc.

## Key differences from PostgreSQL EXPLAIN

| Aspect | PostgreSQL | CedarDB |
| :--- | :--- | :--- |
| Format | Nested `"Plan"` object with `"Node Type"` | Inline `"operator"` + `"physicalOperator"` |
| Actual rows | `"Actual Rows"` × `"Actual Loops"` | `"analyzePlanCardinality"` (already total) |
| Join children | `"Plans"[0]` and `"Plans"[1]` | Explicit `"left"` and `"right"` keys |
| Set operations | `Append` / `Merge Append` nodes | `setoperation` with `"arguments"` array |
| HAVING | Pushed into scan `"Filter"` or `Aggregate` filter | Separate `select` node wrapping `groupby` |
| Timing | Per-node `"Actual Total Time"` | Pipeline-level `"duration"` only |
| Costs | `"Total Cost"`, `"Plan Rows"` | Not reported |
| Index seek | `Index Scan` node type | Same `tablescan` node; `physicalOperator` = `indexscan` |
| FORMATS supported | TEXT, JSON, XML, YAML | TEXT, JSON only |

## Suggested parse strategy for dbprove

1. Execute `EXPLAIN (ANALYZE, FORMAT JSON) <query>` and parse the returned JSON string.
2. The root operator is `data["plan"]`; walk the tree recursively.
3. Dispatch on `node["operator"]` (logical type) for canonical node mapping; use `node["physicalOperator"]` to determine access strategy (e.g. `SEEK` vs `SCAN`).
4. For row counts, prefer `node["analyzePlanCardinality"]` (actual) over `node["cardinality"]` (estimated).
5. The `ius` array at the top level contains all column type information; individual nodes reference IU names by string.
6. Timing is only available per-pipeline from `data["analyzePlanPipelines"]`; there is no per-node wall-clock metric to report.
7. Join orientation: CedarDB always puts the build side as `"left"`. SQL `LEFT JOIN` appears as `"type":"rightouter"`. Map accordingly.
