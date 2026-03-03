# Databricks Execution Plan Parsing

This document tracks the ongoing understanding of Databricks query plans, specifically those retrieved via the SQL UI JSON scraper and `EXPLAIN COST`.

## Plan Sources

1.  **JSON Scraper**: Extracted from the Databricks SQL Warehouse history/query UI. This provides a detailed, nested graph structure representing the physical execution plan (Spark).
2.  **EXPLAIN COST**: Standard Spark SQL command that provides logical and physical plan descriptions along with row estimates (`Statistics(rows=...)`).

## JSON Format Structure (Scraped)

The scraped JSON is a complex object representing the query execution history. The actual plan tree is typically nested within several layers:

-   `graph`: The container for the execution graph.
-   `nodes`: An array of operator nodes. Each node has:
    -   `id`: Unique identifier (string).
    -   `nodeName`: The Spark operator name (e.g., `Scan parquet`, `Filter`, `Sort`, `Project`).
    -   `tag`: Internal Spark execution tag (e.g., `PHOTON_PROJECT_EXEC`).
    -   `metrics`: Actual execution metrics (if the query has run), such as `number of output rows`.
-   `edges`: Defines the relationship between nodes.
    -   **Important Note**: In the Databricks execution graph JSON, edges go from **CONSUMER to PRODUCER**. 
    -   `fromId`: Consumer node ID (closer to the result, acts as the parent in our tree).
    -   `toId`: Producer node ID (closer to the data source, acts as the child in our tree).

### Disjoint Components and roots

Spark query plans often contain multiple disjoint graph components in the JSON (e.g., Result stages vs. Codegen stages). 
-   **Root Identification**: We identify "islands" in the graph by finding nodes that appear as `fromId` but never as `toId`.
-   **Subqueries**: Nodes representing subqueries (name `Subquery` or tag `SUBQUERY`) are identified during edge processing. Their children are collected as separate relational roots and excluded from main root identification.
-   **Sequence Assembly**: If subqueries are present, the main root and all subquery roots are grouped under a top-level `SEQUENCE` node. This reflects the execution of subqueries as separate stages before or during the main query.
-   **Recursive Linking**: To correctly handle move semantics during tree building, we recursively link nodes from the bottom up (producers to consumers).
-   **Terminal Node Identification**: When linking children to a node that represents a canonical technical wrapper (like a Photon Stage), we attach graph-level children to the *leaf* of that technical subtree. However, we ensure that actual relational operators (like `JOIN`, `FILTER`, `AGGREGATE`, `SORT`, `SCAN`, etc.) are treated as **terminal nodes** in this search. This prevents unrelated subtrees (like scalar subqueries used in a `FILTER`) from being incorrectly "pushed down" into the children of another relational operator.

### Reused Exchange and Scan Replication

Databricks/Spark plans often use `Reused Exchange` nodes to avoid redundant scans of the same data (common in star schemas with small dimension tables). In the execution graph JSON, these reused exchanges are often disconnected from the original source stage, appearing as dangling leaves in the relational tree.

To resolve this, the parser implements **Attribute-Based Scan Replication**:
1.  **Two-Pass Architecture**:
    -   **Pass 1 (Pre-scan)**: The parser scans all nodes in the JSON to identify `SCAN` operators. It builds a global mapping of **Output Attributes** (e.g., column names like `r_regionkey`) to the source table and filters.
    -   **Pass 2 (Build)**: During the main plan construction, when a `Reused Exchange` is encountered, the parser looks up its output attributes in the global map.
2.  **Node Cloning**: If a match is found, the parser creates a new `SCAN` node reflecting the original source table and attaches it to the consumer join. This eliminates dangling technical nodes and correctly represents the data flow.

### Data Type Safety and ID Handling

The Databricks parser is designed to be robust against variations in the Spark execution graph JSON:
-   **Node IDs**: Node IDs (`id`, `fromId`, `toId`) can appear as both strings and numbers in the JSON. The parser handles both by converting them to a canonical string representation.
-   **Null Safety**: Spark's JSON often contains `null` for fields that are typically strings (like `name`, `tag`, or metadata `value`). All string extractions use a `safeGetString` helper to prevent type errors.
-   **Graceful Node Parsing**: Individual node parsing is wrapped in `try-catch` blocks. If a specific node fails to parse, it is treated as a "skipped" node (transparently passing children up), ensuring the overall plan tree can still be constructed.

### Descriptive Artifact Naming and Storage

The Databricks parser supports descriptive artifact naming and storage via centralized methods in `ConnectionBase`. When explaining a query:
- `getArtefact(name, extension)`: Automatically looks for `artifacts_path/<engine>_<name>_<extension>` (e.g., `databricks_TPCH-Q01_json`).
- `storeArtefact(name, extension, content)`: Automatically stores artifacts using the same `<engine>_<name>_<extension>` pattern.

This centralized logic ensures consistency across different database engines and simplifies the implementation of the `explain` method.

## Mapping to Canonical Plan (dbprove)

We map Spark operators to the canonical `sql::explain::Node` types using both the `nodeName` and `tag` fields:

| Spark Operator / Tag | `dbprove` Node Type | Notes |
| :--- | :--- | :--- |
| `Relation` / `Scan` | `SCAN` | Base table access. Actual rows in `metrics` as `NUMBER_OUTPUT_ROWS`. Pushed filters extracted from `PUSHED_FILTERS`, `PARTITION_FILTERS`, `DATA_FILTERS`, or `FILTERS` metadata. Handles `LocalTableScan` by mapping to `LocalTable` and using `LocalRelation` estimates. |
| `Filter` | `FILTER` | Predicate application. Conditions extracted from `CONDITION` metadata. Mapped to `Selection` node. |
| `Project` | `PROJECT` | Column selection/transformation. Projected columns extracted from `PROJECTION` metadata. Mapped to `Select` node. |
| `Sort` | `SORT` | Ordering. Keys extracted from `SORT_ORDER` metadata. |
| `Aggregate` | `AGGREGATE` | Grouping and aggregation. Keys and functions extracted from `GROUPING_EXPRESSIONS` and `AGGREGATE_EXPRESSIONS` metadata. Strategy is `SIMPLE` if group keys are empty, otherwise `HASH`. |
| `Join` | `JOIN` | Join operations. Supports Inner, Outer (Left/Right/Full), Semi, and Anti (Left/Right variants). |
| `Exchange` / `Shuffle` | `DISTRIBUTION` | Data movement. Map to `DISTRIBUTE`. Strategy and keys extracted from `PARTITIONING_TYPE`, `PARTITIONING_EXPRESSIONS`, or `SHUFFLE_ATTRIBUTES` metadata. |
| `Union` | `UNION` | Map to `sql::explain::Union(Union::Type::ALL)`. Row estimates from `ctx.row_estimates`. |
| `Limit` / `TopK` | `LIMIT` | Limit clause. Count extracted from `LIMIT` metadata or `GlobalLimit` / `LocalLimit` estimates. `PhotonTopK` is mapped to `SORT` + `LIMIT` if sort order is present. |
| `Adaptive Plan` / `Stage` / `Subquery` | `PROJECT` | Structural wrappers. Included to preserve hierarchy. |
| `Photon Result Stage` | `PROJECT` | Result collection stage. |
| `Arrow Conversion` | (ignored) | Ignored during parsing. Technical node for data format conversion. |
| `Arrow Result Stage` | (ignored) | Ignored during parsing. Technical node for result delivery. |
| `Columnar to Row` | (ignored) | Ignored during parsing. Technical node for data format conversion. |
| `Columnar To Row` | (ignored) | Ignored during parsing. Technical node for data format conversion. |
| `Result Query Stage` | (ignored) | Ignored during parsing. Technical node for final result delivery. |
| `Reused Exchange` | (ignored) | Ignored. Spark AQE optimization for reusing shuffle results. |
| `Shuffle Map Stage` | (ignored) | Ignored. Technical Spark stage for shuffle writes. |

## Post-Processing

The parser applies three post-processing steps to the plan tree before returning it:

1.  **Estimate Propagation**: If a node is missing `rows_estimated` or `rows_actual` (value is `NAN`), it attempts to propagate these values from its children.
2.  **Relational No-Op Collapsing**: Intermediate `SELECT` or `PROJECTION` nodes that have exactly one child are aggressively collapsed if they don't perform relational filtering. For Databricks, this includes:
    - Nodes with empty output columns.
    - Nodes where the row count matches the child (within rounding).
    - Nodes where either count is `NaN`.
    - Technical `SELECT` nodes that merely wrap `JOIN`, `SCAN`, or `DISTRIBUTE` operators.
    - This process is implemented as a two-pass tree traversal and applied iteratively to remove deep chains of technical wrappers. `DISTRIBUTE` nodes are explicitly preserved to show data movement.
3.  **Top-Level Project Removal**: If the root node is a `PROJECT` or `SELECT` node with a single child, it is removed. This typically represents moving data to the client.

## Join Keys Extraction

Join keys are extracted from the `metaData` array within the Spark execution graph JSON.
Specifically, we look for items with `key` as `LEFT_KEYS` and `RIGHT_KEYS`. These keys are typically stored in the `values` array of the metadata item. We combine them into a canonical `ON left = right` condition.

## Row Estimates Integration

For a simple scan query (`SELECT val FROM test.pk`), the following was observed in the JSON:
-   **Node Name**: `Scan ybaws.test.pk` (includes catalog/schema).
-   **Node Tag**: `UNKNOWN_DATA_SOURCE_SCAN_EXEC`.
-   **Metrics**: Actual rows are stored in an object with `key: "NUMBER_OUTPUT_ROWS"` and `label: "Number of output rows"`. The value is a string (e.g., `"5"`).
-   **Hierarchy**: The scan is often a leaf node, or wrapped in a `WholeStageCodegen` or `PhotonResultStage`.

## JSON Node Observation: Project

-   **Node Name**: `Project` or `PhotonProject`.
-   **Node Tag**: `PHOTON_PROJECT_EXEC`.
-   **Metrics**: Also contains `Number of output rows`.

## Row Estimates Integration

Spark's logical estimates (from `EXPLAIN COST`) are matched against the physical nodes in the JSON graph. This is currently done by operator type and position where possible.
The parser currently extracts estimates for:
- `Relation` (mapped to `SCAN`)
- `Aggregate`
- `Sort`
- `Project`
- `Filter`
- `Join`
- `Union`
- `Limit`

## Strategy: Incremental Parsing via Theorems

We use `src/theorem/test_theorem.cpp` and `src/theorem/plan/prove.cpp` to run controlled queries (e.g., TPC-H Q01) and capture their plans as artifacts. 

-   **Test Driven**: By observing how different SQL constructs (joins, subqueries, CTEs) manifest in the JSON, we can incrementally teach the parser to handle more complex plans.
-   **Artifacts**: Using the `-a/--artifacts` flag allows us to work with stable, offline copies of the JSON for iterative development.