# SQL Server Execution Plan Parsing

This document tracks the ongoing understanding of SQL Server query plans, specifically those retrieved via `SET STATISTICS XML ON`.

## Plan Sources

1.  **STATISTICS XML**: Standard SQL Server command that provides a detailed, nested XML representation of the query execution plan, including both estimated and actual runtime metrics (if the query is executed).

## Plan Retrieval

The `explain` method in the SQL Server driver implements the following flow:
1.  **Artifact Check**: It first checks for a cached XML plan in the specified artifacts directory (if the `-a/--artifacts` flag is used).
2.  **STATISTICS XML**: If no artifact is found, it executes `SET STATISTICS XML ON`.
3.  Executes the target SQL statement.
4.  Drains the result sets (as the query execution produces data).
5.  Fetches the next result set, which contains the XML plan.
6.  Executes `SET STATISTICS XML OFF`.
7.  **Artifact Storage**: The retrieved XML is stored in the artifacts directory for future use.

#### Plan Artifacts
To make analysis and debugging easier, you can cache SQL Server plan artifacts using the `-a/--artifacts <path>` flag. 
```bash
dbprove -e mssql ... -a ./my_artifacts
```
When this flag is used, `dbprove` will first check the specified directory for cached files (named `sql server_<name>_xml`). If found, it will skip all remote calls and use the local files. If not found, it will perform the full explain flow and save the results for next time.

## XML Format Structure

The SQL Server XML plan (`ShowPlanXML`) is a hierarchical structure of operators:

-   `RelOp`: The primary container for a relational operator. Attributes include:
    -   `PhysicalOp`: The physical implementation (e.g., `Clustered Index Scan`, `Hash Match`).
    -   `LogicalOp`: The logical relational operation (e.g., `Inner Join`, `Aggregate`).
    -   `EstimateRows`: The optimizer's estimated row count.
-   `RunTimeInformation`: Contains actual execution metrics (if available).
    -   `RunTimeCountersPerThread`: Tracks `ActualRows` at the thread level.
-   `OutputList`: Defines the columns produced by the operator.
-   `ScalarOperator`: Contains expressions (filters, projections, join conditions) as stringified SQL-like text in the `ScalarString` attribute.

### Node Mapping (Canonical Plan)

We map SQL Server operators to the canonical `sql::explain::Node` types:

| Physical / Logical Op | `dbprove` Node Type | Notes |
| :--- | :--- | :--- |
| `Index Scan` / `Table Scan` | `SCAN` | Base table access. Mapped to `Scan` with `SCAN` strategy. If `Storage="ColumnStore"`, always use `SCAN` strategy even for `IndexScan` elements. |
| `Index Seek` | `SCAN` | Base table access with a predicate. Mapped to `Scan` with `SEEK` strategy. |
| `Filter` | `FILTER` | Predicate application. Mapped to `Selection`. |
| `Top` | `LIMIT` | Limit clause. Mapped to `Limit`. |
| `Sort` | `SORT` | Ordering. Mapped to `Sort`. |
| `Segment` | `SELECT` | Window function segmentation. Mapped to `Select`. |
| `Hash Match` (Aggregate) | `AGGREGATE` | Grouping and aggregation. Mapped to `GroupBy` with `HASH` strategy. Grouping keys extracted from `HashKeysBuild` or `HashKeysProbe`. If no grouping columns, strategy becomes `SIMPLE`. |
| `Stream Aggregate` | `AGGREGATE` | Grouping and aggregation. Mapped to `GroupBy` with `SORT_MERGE` strategy. Grouping keys extracted from `GroupBy`. If no grouping columns, strategy becomes `SIMPLE`. |
| `Merge Join` | `JOIN` | Join operation. Mapped to `Join` with `MERGE` strategy. Join keys extracted with priority to `Residual`. |
| `Hash Match` (Join) | `JOIN` | Join operation. Mapped to `Join` with `HASH` strategy. Join keys extracted with priority to `ProbeResidual`. |
| `Nested Loops` | `JOIN` | Join operation. Mapped to `Join` with `LOOP` strategy. Join keys extracted from `SeekPredicates`. |
| `Adaptive Join` | `JOIN` | Join operation. Mapped to `Join` with `HASH` strategy (representing the estimated join type). Join keys extracted with priority to `ProbeResidual`. |
| `Table Spool` (Producer) | `MATERIALISE` | Creation of a temporary result set. |
| `Table Spool` (Consumer) | `SCAN MATERIALISED` | Reading from a previously created spool. |
| `Compute Scalar` | `PROJECT` | Column transformation. Mapped to `Projection`. |
| `Parallelism` | `DISTRIBUTE` | Data movement. Mapped to `Distribute` with `GATHER`, `HASH`, or `BROADCAST` strategy. |

### Post-Processing

The parser applies several post-processing steps to ensure accuracy:
1.  **Row Count Summing**: Actual row counts are summed across all execution threads in `RunTimeCountersPerThread`. The parser also checks `BatchModeOnRowStoreCounters` and `ActualRows`/`ActualRowsRead` attributes directly on `RunTimeInformation`.
2.  **Rows Read and Batch Mode Capture**: 
    - For scan nodes, the parser prefers `ActualRowsRead` over `ActualRows` if it's larger, ensuring work done is captured even with push-down aggregation.
    - In Batch Mode scans, if `ActualRows` is 0, the parser looks for `ActualLocallyAggregatedRows` to capture rows processed during partial aggregation.
3.  **Aggressive Propagation**: A `fixProjection` pass ensures non-filtering operators (Projections, Sorts, Selects, etc., and now including `SCAN` if child is available) inherit row counts from their children if they report zero or missing counts.
4.  **Expression Cleaning and Deduplication**: Scalar expressions in filters and projections are cleaned of SQL Server-specific noise:
    -   Redundant brackets (e.g., `[column]`) and schema prefixes (e.g., `[db].[schema].`) are removed.
    -   Redundant aliases (e.g., `column AS column`) are pruned to keep the plan concise.
    -   Known internal functions like `PROBE` (Bloom filters) are mapped to standard `BLOOM()` notation.
    -   **Structured Expression Parsing**: Instead of just using `ScalarString`, the parser now traverses `ScalarOperator` elements to extract structured `ColumnReference` information, which includes table and alias qualifiers.
    -   **Alias-Aware Cleaning**: `cleanExpression` and `cleanFilter` now intelligently preserve `Alias.Column` and `Table.Column` while stripping database and schema prefixes (e.g., `[db].[schema].`).
    -   **Filter Deduplication**: Repeated filter conditions (often caused by push-down filters appearing in multiple places in the XML) are deduplicated by splitting on `AND` and normalizing components.
5.  **Join Filter Extraction**: Join conditions are extracted using a prioritized search:
    -   For **Merge Joins**, the `Residual` element is preferred if present.
    -   For **Hash** and **Adaptive Joins**, the `ProbeResidual` element is preferred.
    -   If these residuals are missing, the parser falls back to explicit equi-join keys (`HashKeysBuild`/`Probe` or `InnerSideJoinColumns`/`OuterSideJoinColumns`).
    -   This "first-match-wins" strategy ensures a clean, non-redundant canonical plan while still capturing all necessary filters.
5.  **Dynamic Adaptive Join Selection**: For `Adaptive Join` and `Nested Loops` (when part of an adaptive plan), the parser inspects runtime metrics (`ActualExecutions`) to only include children that were actually executed. This prevents logically impossible 0-row "ghost" subtrees from appearing in the canonical plan.
6.  **Parallelism Handling**: `Parallelism` nodes (Gather, Repartition, and Distribute Streams) are mapped to canonical `DISTRIBUTE` nodes to preserve the relational data flow.
7.  **Table Alias Extraction**: For all scan operators, the parser extracts the `Alias` attribute from the `Object` element in the XML. This ensures that aliased tables in complex queries (like TPC-H Q21) are correctly identified in the canonical plan (e.g., `lineitem AS l1`).
8.  **Materialized Scan Reference Extraction**: For `Table Spool` operators that reference a previously spooled subtree, the parser now extracts the `PrimaryNodeId`. This allows 'SCAN MATERIALISED' nodes to indicate their lineage (e.g., `SCAN MATERIALISED (NODE 5)`), improving the clarity of plans using spools.

## Driver Discovery

The MSSQL connector uses a robust mechanism to find the best available ODBC driver on the system:
1.  **Auto-Discovery**: It uses `SQLGetInstalledDrivers` (with a fallback to `dlopen` on macOS) to list all drivers.
2.  **Prioritized Search**: It looks for drivers in order of preference: `ODBC Driver 18 for SQL Server`, `17`, `13.1`, `13`, `11`, `Native Client 11.0`, `Native Client 10.0`, and finally `SQL Server`.
3.  **Path Resolution**: It attempts to resolve the friendly driver name to an absolute library path using `SQLGetPrivateProfileString` or common installation paths.
4.  **Caching**: The discovery result is cached for the duration of the process to avoid redundant searches and logging.

## Known Issues and Limitations

- **Complex Expressions**: Some complex scalar expressions might still contain SQL Server-specific aliases or internal functions that are not fully cleaned.

### macOS Requirements

To use this connector on macOS, you must install the Microsoft ODBC Driver 18 for SQL Server:
```bash
brew tap microsoft/mssql-release
brew install msodbcsql18 mssql-tools18
```
The driver is required even when connecting to a SQL Server instance running in Docker.

### Running with Docker

To start a SQL Server instance for testing:
```bash
cd docker
docker-compose up -d mssql
```

Default credentials:
- **User**: `sa`
- **Password**: `YourStrong!Passw0rd`

Data is persisted in `run/mount/mssql`.
