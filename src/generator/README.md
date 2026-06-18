# Data Generation

Data is "generated" by downloading CSV and Parquet files from a well known bucket location.

The register generation, you call:

```c
REGISTER_TABLE('[table]', '[schema]', [ddl], [expected row count], [expected_file_count]);
```

The registration still carries table metadata such as DDL today, and we keep room for explicit primary/foreign key metadata alongside it.

The convention is that the files needed to populated `[table]` is that the files needed for the table are located in the path

- `S3://[object_path]/[schema_path]`

The `[object_path]` is `S3://sql-arena` by default, but can be overriden from the `dbprove` command line.

The `schema_path` is simply the `[schema]` but with underscores replaced with slash (`/`)


If `[expecte_file_count]`=1, the table data
is locate in these two files:

- `S3://[object_path]/[schema_path]/[table].csv.zip`
- `S3://[object_path]/[schema_path]/[table].parquet`

If `[expecte_file_count]`>1 the table data is located in:


- `S3://[object_path]/[schema_path]/[table]_[NNNN].csv.zip`
- `S3://[object_path]/[schema_path]/[table]_[NNNN].parquet`

With `NNNN` being the zero padded, 1 based number from 1 to `[expected_file_count]`.

## Ensuring data is present for an engine

When `ensureTable` is called one of two things happen.

If the requested storage variant is `native` a local cache in `table_data` (located in the directory where `dbprove` is invoked) is consulted. If the data is not present, it will be downloaded from the object store. 
Once the cache is populated, the data is either bulk loaded into the engine (via the `bulkLoad` call of the driver) or mounted directly from `table_data` as parquet or CSV (depending on what format the engine can understand)

The local `table_data` cache mirrors the registered schema:

- `table_data/[schema]/[table].csv`
- `table_data/[schema]/[table].parquet`
- `table_data/[schema]/[table]_[NNNN].csv`
- `table_data/[schema]/[table]_[NNNN].parquet`

These naming and path conventions are shared with engine loaders through
`dbprove/common/table_data_conventions.h` so engines can safely rewrite DDL,
construct mounted file paths, or implement bulk load without duplicating the
rules.

If the requested storage variant is `iceberg` the table is set up to point directly at the object storage.

## ConnectionFactory contract

The generator itself does not talk to an engine directly. It is given a
`sql::ConnectionFactory`, and uses that factory to obtain
`sql::ConnectionBase` instances as it ensures schemas and asks the engine to
construct tables from staged files.

The required factory callback is:

```c++
std::unique_ptr<sql::ConnectionBase> create();
```

The returned connection must support the following callbacks because the
generator calls them as part of `ensure(...)` and `ensureDataset(...)`:

- `createSchema(schema_name)`
  Used once per schema before loading dataset tables.
- `tableRowCount(table_name)`
  Used to detect whether a table already exists and whether its row count
  matches the registered expectation.
- `constructTable(ddl, source_stems, storage_variant)`
  Used only when the table is missing.
  The Generator provides generic DDL that the engine may translate or ignore.
  `source_stems` are the canonical staged file paths without `.csv` or
  `.parquet`, for example:
  - `table_data/tpch_sf1/customer`
  - `table_data/tpch_sf1/lineitem_0001`
  The engine is free to use those stems however it wants:
  - append `.csv` and call `bulkLoad(...)`
  - append `.parquet` and create an external or mounted table
  - choose behavior based on `storage_variant`

So the generic ensure flow is:

1. `createSchema(...)` for each schema in the dataset.
2. `tableRowCount(...)` to check whether the table is already ready.
3. `constructTable(...)` if the table is missing.
5. `tableRowCount(...)` again to verify the final row count.

The default implementation of `constructTable(...)` is the classic native
behavior:

1. parse the DDL and render an engine-specific `CREATE TABLE`
2. execute that `CREATE TABLE`
3. append `.csv` to the staged stems
4. call `bulkLoad(...)`

Engines that prefer mounted parquet or Iceberg-style setup can override
`constructTable(...)` and ignore the CSV path entirely.
