# PostgreSQL connector

Currently, this connector uses `libpq` - which is dog slow. However, it is the library that is most commonly 
used to access PostgreSQL (it is the underpinnings of most publicly shipped JDBC and ODBC drivers).

`libpq` lacks a separate, standalone repo - which is sad. This means that we need to `vcpkg` all of the `PostgreSQL`
which is a rather beefy library

## Execution Plan Parsing

This document tracks the ongoing understanding of PostgreSQL query plans, retrieved via `EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON)`.

### Plan Sources

The `explain` method in the PostgreSQL driver implements the following flow:
1.  **Artifact Check**: It first checks for a cached JSON plan in the specified artifacts directory (if the `-a/--artifacts` flag is used).
2.  **EXPLAIN Command**: If no artifact is found, it executes `EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) <statement>`.
    -   `ANALYZE`: Executes the query and displays actual run times and other statistics.
    -   `VERBOSE`: Displays additional information, such as the output column list for each node.
    -   `FORMAT JSON`: Requests the output in a structured JSON format for easier programmatic parsing.
3.  **Artifact Storage**: The retrieved JSON is stored in the artifacts directory (as `postgresql_<name>.json`) for future use.
4.  **JSON Parsing**: The resulting JSON string is parsed using `nlohmann::json`.

#### Plan Artifacts
To make analysis and debugging easier, you can cache PostgreSQL plan artifacts using the `-a/--artifacts <path>` flag. 
```bash
dbprove -e postgres ... -a ./my_artifacts
```
When this flag is used, `dbprove` will first check the specified directory for cached files. If found, it will skip all remote calls and use the local files. If not found, it will perform the full explain flow and save the results for next time.

### JSON Format Structure

PostgreSQL EXPLAIN JSON output is a nested structure where each node contains:
-   `Node Type`: The type of operation (e.g., `Seq Scan`, `Hash Join`).
-   `Plan Rows`: The estimated number of rows.
-   `Actual Rows`: The actual number of rows produced (per loop).
-   `Actual Loops`: The number of times the node was executed.
-   `Total Cost`: The estimated total cost to execute the node.
-   `Output`: A list of columns produced by the node.
-   `Plans`: A list of child nodes (sub-plans).

#### Node Mapping (Canonical Plan)

We map PostgreSQL operators to the canonical `sql::explain::Node` types:

| PostgreSQL Node Type | `dbprove` Node Type | Notes |
| :--- | :--- | :--- |
| `Seq Scan` | `SCAN` | Sequential table scan. Strategy is `SCAN`. |
| `Index Scan`, `Index Only Scan` | `SCAN` | Index-based access. Strategy is `SEEK`. |
| `Bitmap Heap Scan`, `Bitmap Index Scan` | `SCAN` | Bitmap index access. Mapped to `Scan` with `SEEK` strategy. Inner bitmap operations (BitmapAnd, BitmapOr) are currently collapsed into the top-level scan. |
| `Hash Join` | `JOIN` | Join operation. Mapped to `Join` with `HASH` strategy. |
| `Nested Loop` | `JOIN` | Join operation. Mapped to `Join` with `LOOP` strategy. If it has an `Index Scan` child, the join condition is often extracted from the child's `Index Cond`. |
| `Merge Join` | `JOIN` | Join operation. Mapped to `Join` with `MERGE` strategy. |
| `Sort` | `SORT` | Ordering. Mapped to `Sort`. Descending columns are identified by the `" DESC"` suffix in the `Sort Key` string. |
| `Limit` | `LIMIT` | Limit clause. Mapped to `Limit`. |
| `Aggregate` | `AGGREGATE` | Grouping and aggregation. Mapped to `GroupBy`. Strategy is determined by the `Strategy` field (`Hashed` -> `HASH`, `Sorted` -> `SORT_MERGE`, `Plain` -> `SIMPLE`). |
| `Result` | `SELECT` | Constant or simple projection. Mapped to `Select`. |
| `Append` | `UNION` | Union of multiple sub-plans. Mapped to `Union` with `ALL` type. |
| `Merge Append` | `UNION` | Distinct union of sorted sub-plans. Mapped to `Union` with `DISTINCT` type. |

#### Post-Processing and Adjustments

The parser applies several logic adjustments to normalize PostgreSQL plans:

1.  **Loop Correction**: PostgreSQL reports `Actual Rows` as an *average* per loop. To get the true total row count, the parser multiplies `Actual Rows` by `Actual Loops`.
    -   It also scales `Plan Rows` (estimated) by `Actual Loops` to ensure fair comparison between estimate and actual in the rendered plan.
    -   Since PostgreSQL might report 0 `Actual Rows` due to integer rounding of a small average, the parser ensures the count is at least equal to `Actual Loops` if `Actual Rows` is 0 but `Actual Loops` > 0.
2.  **Join Strategy Normalization**: 
    -   If a join has no condition, it is explicitly mapped to `Join::Type::CROSS`.
    -   **Join Flipping**: PostgreSQL Loop joins often have the lookup table as the second child. To maintain consistency in rendering (where the "build" or "inner" side is usually first), the parser reverses the children for joins.
3.  **InitPlan and SubPlan Handling**: 
    - PostgreSQL "InitPlans" (sub-plans that run before the main query) are captured.
    - Correlated "SubPlans" attached to join operators are identified as "extra" children (beyond the expected 2).
    - These extra sub-plans are hoisted into a `Sequence` node at the root to show them executing before the main query plan, ensuring a clean two-child join structure.
    - Sub-plans attached directly to `Scan` nodes are wrapped in a synthesized `LEFT SEMI JOIN` or `LEFT ANTI JOIN` to correctly represent their filtering logic.
    - For scan filters that reference hashed subplans (for example `NOT (ANY (partsupp.ps_suppkey = (hashed SubPlan 1).col1)))`), we now extract a join predicate for the synthesized join using the scan-side key and the first projected subplan column (for example `ps_suppkey = s_suppkey`).
    - For subplan child nodes, we now store total fetched rows as `Actual Rows * Actual Loops` (with a floor of `Actual Loops`) so correlated loop execution is represented correctly in rendered row counts.
4.  **Condition Extraction**:
    -   For `Nested Loop` joins involving an index, the join condition is extracted from the `Index Cond` of the inner side scan.
    -   `Filter` conditions and `Join Filter` conditions are cleaned and assigned to the node.
5.  **Bitmap Scan Collapsing**: PostgreSQL's complex bitmap index scan trees (BitmapAnd/Or) are collapsed into a single `SCAN` node to keep the canonical plan readable while still indicating the index-based access.

### Running with Docker

To start a PostgreSQL instance for testing:
```bash
cd docker
docker-compose up -d postgresql
```

Default credentials:
-   **User**: `postgres`
-   **Password**: `postgres`
-   **Database**: `bench`

Data is persisted in `run/mount/postgresql`.

Note that when benchmarking, you will need to change the default configuration of PostgreSQL, the reference script is
in `configure.sql`. For testing the connector, use `configure_test.sql`
