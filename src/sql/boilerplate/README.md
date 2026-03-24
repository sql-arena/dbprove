# Boilerplate Driver

This directory is the starting template for a new SQL driver.

When creating a driver:

1. Copy this directory to `src/sql/<engine>/`.
2. Rename the namespace, target names, and include paths from `boilerplate` to your engine name.
3. Fill in the real connection, result, row, and optional explain logic.
4. Register the driver in `src/sql/README.md`'s checklist:
   - `src/sql/include/dbprove/sql/engine.h`
   - `src/sql/engine.cpp`
   - `src/sql/connection_factory.cpp`
   - `src/sql/CMakeLists.txt`

## File Roles

- `connection.h` and `connection.cpp`: engine connection lifecycle, execution, fetching, bulk load, and explain entry points.
- `result.h` and `result.cpp`: result-set ownership and cursor movement.
- `row.h` and `row.cpp`: row access helpers backed by the result implementation.
- `explain.cpp`: canonical `sql::explain::Plan` construction if the engine supports explain parsing.
- `CMakeLists.txt`: per-driver target name, sources, and external library linkage.

Most real drivers also add:

- `README.md` for setup, auth, and limitations.
- `parsers.h` for engine-specific value conversion helpers.
- `tune/` scripts when theorem datasets need engine-specific tuning objects.
