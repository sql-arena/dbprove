set(_targetName "dbprove_sql_dudkdb")
add_library(${_targetName} STATIC)
add_library(dbprove::sql::duckdb ALIAS ${_targetName})
find_package(duckdb REQUIRED)
add_compile_definitions(DUCKDB_STATIC_LINK)


target_sources(${_targetName} PRIVATE
        result.cpp
        connection.cpp
        row.cpp)

target_link_libraries(${_targetName}
        PRIVATE
        duckdb_static
        dbprove::sql::driver
        dbprove::sql)
target_compile_definitions(${_targetName} PRIVATE DUCKDB_STATIC_LINK)


print_target_info(duckdb)