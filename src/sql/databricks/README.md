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

### Disjoint Components and Roots

Spark query plans often contain multiple disjoint graph components in the JSON (e.g., Result stages vs. Codegen stages). 
-   **Root Identification**: We identify "islands" in the graph by finding nodes that appear as `fromId` but never as `toId`.
-   **Prioritization**: Among root candidates, we prioritize those with tags containing `RESULT` or `SINK`.
-   **Recursive Linking**: To correctly handle move semantics during tree building, we recursively link nodes from the bottom up (producers to consumers).

## Mapping to Canonical Plan (dbprove)

We map Spark operators to the canonical `sql::explain::Node` types using both the `nodeName` and `tag` fields:

| Spark Operator / Tag | `dbprove` Node Type | Notes |
| :--- | :--- | :--- |
| `Scan` / `Relation` | `SCAN` | Base table access. Actual rows in `metrics` as `NUMBER_OUTPUT_ROWS`. |
| `Filter` | `FILTER` | Predicate application. |
| `Project` | `PROJECT` | Column selection/transformation. |
| `Sort` | `SORT` | Ordering. Map to `SORT`. |
| `Aggregate` | `AGGREGATE` | Grouping and aggregation. Keys and functions extracted from `GROUPING_EXPRESSIONS` and `AGGREGATE_EXPRESSIONS` metadata. Strategy is `SIMPLE` if group keys are empty, otherwise `HASH`. |
| `Join` | `JOIN` | Join operations. Supports Inner, Outer (Left/Right/Full), Semi, and Anti (Left/Right variants). |
| `Exchange` / `Shuffle` | `DISTRIBUTION` | Data movement. Map to `DISTRIBUTE`. Strategy and keys extracted from `PARTITIONING_TYPE` and `PARTITIONING_EXPRESSIONS` metadata. |
| `Union` | `UNION` | Map to `sql::explain::Union(Union::Type::ALL)`. Row estimates from `ctx.row_estimates`. |
| `Adaptive Plan` / `Stage`| `PROJECT` | Structural wrappers. Included to preserve hierarchy. |
| `Photon Result Stage` | `PROJECT` | Result collection stage. |

## Post-Processing

The parser applies two post-processing steps to the plan tree before returning it:

1.  **Top-Level Project Removal**: If the root node is a `PROJECT` or `SELECT` node with a single child, it is removed. This typically represents moving data to the client and is not interesting for theorem proving.
2.  **Estimate Propagation**: If a node is missing `rows_estimated` or `rows_actual` (value is `NAN`), it attempts to propagate these values from its children.

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

## Strategy: Incremental Parsing via Theorems

We use `src/theorem/test_theorem.cpp` and `src/theorem/plan/prove.cpp` to run controlled queries (e.g., TPC-H Q01) and capture their plans as artifacts. 

-   **Test Driven**: By observing how different SQL constructs (joins, subqueries, CTEs) manifest in the JSON, we can incrementally teach the parser to handle more complex plans.
-   **Artifacts**: Using the `-a/--artifacts` flag allows us to work with stable, offline copies of the JSON for iterative development.