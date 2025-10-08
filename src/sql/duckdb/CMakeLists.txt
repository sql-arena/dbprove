set(_targetName "dbprove_sql_dudkdb")
add_library(${_targetName} STATIC)
add_library(dbprove::sql::duckdb ALIAS ${_targetName})

set(DuckDB_USE_STATIC_LIBS ON)
add_compile_definitions(DUCKDB_STATIC_LINK)
find_package(duckdb CONFIG REQUIRED)


target_sources(${_targetName} PRIVATE
        result.cpp
        connection.cpp
        row.cpp
        explain.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        duckdb_static
        dbprove::sql::driver
        dbprove::sql)


print_target_info(duckdb)