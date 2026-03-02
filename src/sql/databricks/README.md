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
    -   `nodeDescription`: Detailed description (often containing schema, expressions, and metadata).
    -   `metrics`: Actual execution metrics (if the query has run), such as `number of output rows`.
-   `edges`: Defines the producer-to-consumer relationship between nodes.
    -   **Important Note**: In the Databricks execution graph JSON, edges actually go from **CONSUMER to PRODUCER**. 
    -   `fromId`: Consumer node ID (closer to the result).
    -   `toId`: Producer node ID (closer to the data source).

## Mapping to Canonical Plan (dbprove)

We map Spark operators to the canonical `sql::explain::Node` types:

| Spark Operator | `dbprove` Node Type | Notes |
| :--- | :--- | :--- |
| `Scan` / `Relation` | `SCAN` | Base table access. Actual rows in `metrics` as `Number of output rows`. |
| `Filter` | `FILTER` | Predicate application. |
| `Project` | `PROJECT` | Column selection/transformation. |
| `Sort` | `PHOTON_SORT_EXEC` | Ordering. Map to `SORT`. |
| `Aggregate` / `GroupingAggregate` | `AGGREGATE` | Grouping and aggregation. |
| `HashJoin` / `BroadcastHashJoin` | `JOIN` | Join operations. |
| `Shuffle` / `Exchange` | `EXCHANGE` | Data movement between nodes. |

## JSON Node Observation: Scan

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

## Strategy: Incremental Parsing via Theorems

We use `src/theorem/test_theorem.cpp` and `src/theorem/plan/prove.cpp` to run controlled queries (e.g., TPC-H Q01) and capture their plans as artifacts. 

-   **Test Driven**: By observing how different SQL constructs (joins, subqueries, CTEs) manifest in the JSON, we can incrementally teach the parser to handle more complex plans.
-   **Artifacts**: Using the `-a/--artifacts` flag allows us to work with stable, offline copies of the JSON for iterative development.