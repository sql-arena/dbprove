# SQL Tune Script Layout

This directory now uses dataset-specific tune scripts per engine.

## Structure

Tune scripts live under each engine in a `tune/` folder:

- `src/sql/<engine>/tune/<dataset>.sql`
- `src/sql/<engine>/tune/<dataset>_drop.sql`

Examples:

- `src/sql/mssql/tune/tpch.sql`
- `src/sql/mssql/tune/tpch_drop.sql`
- `src/sql/postgresql/tune/tpch.sql`
- `src/sql/postgresql/tune/tpch_drop.sql`
- `src/sql/databricks/tune/tpch.sql`
- `src/sql/databricks/tune/tpch_drop.sql`

## Execution Behavior

When a theorem calls `ensureDataset("<dataset>")`, dbprove:

1. Ensures the dataset tables are present.
2. Looks for `src/sql/<engine>/tune/<dataset>.sql`.
3. Executes it if present.

`<dataset>_drop.sql` scripts are for safe teardown and are not auto-executed by `ensureDataset`.

## Script Requirements

- Scripts must be idempotent (safe to run repeatedly).
- Scripts should guard object creation with `IF EXISTS` / catalog checks.
- Drop scripts should be safe against partially existing datasets.
- Engine-specific syntax should be used (for example, SQL Server drop order for dependent tables).
