# Canonical Explain Model

This folder contains the shared execution-plan model used by all SQL engines.

## Purpose

Engine-specific explain parsers should convert native plan formats into these canonical nodes so rendering and metrics are engine-agnostic.

## Core Files

- `src/sql/explain/node.cpp` and `src/sql/explain/explain_nodes.h`: base tree node model.
- `src/sql/explain/plan.cpp`: rendering, row-volume metrics, and mis-estimation aggregation.
- `src/sql/explain/join.cpp`: join SQL reconstruction and join-type rendering.
- `src/sql/explain/scan_materialised.cpp` and `src/sql/explain/materialise.cpp`: materialized subtree producer/consumer nodes.

## Canonical Node Families

- Access: `SCAN`, `SCAN MATERIALISED`
- Relational transforms: `FILTER`, `PROJECT`, `GROUP BY`, `SORT`, `LIMIT`
- Set and flow: `JOIN`, `UNION`, `SEQUENCE`, `DISTRIBUTE`
- Internal helpers: `SELECT`, `MATERIALISE`, `SCAN EMPTY`

## Engine Parser Contract

Each engine parser should:

1. Build a tree of canonical nodes with stable parent/child relationships.
2. Populate row metadata when available. If unavailable, use an engine-specific fallback (for example subtree `COUNT(*)` reconstruction).
3. Normalize engine expression syntax into renderable SQL.
4. Run post-processing passes needed to correct engine-specific quirks.

## Rendering Notes

- `Plan::flipJoins()` is available because several engines emit join children opposite of canonical render order.
- Join SQL reconstruction applies alias-based fixes for ambiguous self-equality predicates.
- Row-volume metrics (`rowsScanned`, `rowsJoined`, `rowsProcessed`, and others) are computed from canonical node types in `plan.cpp`.

## New Session Quick Start

To debug an engine explain issue, read:

1. `src/sql/<engine>/README.md`
2. `src/sql/<engine>/explain.cpp`
3. `src/sql/explain/plan.cpp`
4. `src/sql/explain/join.cpp`
