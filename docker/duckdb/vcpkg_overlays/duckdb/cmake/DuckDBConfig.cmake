# Config file for DuckDB package

set(DuckDB_EXTENSIONS "core_functions;parquet;jemalloc")

include(CMakeFindDependencyMacro)
find_dependency(Threads)

get_filename_component(DuckDB_CMAKE_DIR "${CMAKE_CURRENT_LIST_FILE}" PATH)
set(DuckDB_INCLUDE_DIRS "${DuckDB_CMAKE_DIR}/../../../include")

if(NOT TARGET duckdb AND NOT DuckDB_BINARY_DIR)
    include("${DuckDB_CMAKE_DIR}/DuckDBExports.cmake")
endif()

if(DuckDB_USE_STATIC_LIBS)
    set(DuckDB_LIBRARIES duckdb_static)
else()
    set(DuckDB_LIBRARIES duckdb)
endif()
