set(_targetName "dbprove_sql_postgres")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::postgres ALIAS ${_targetName})
find_package(PostgreSQL REQUIRED)

print_target_info(PostgreSQL::PostgreSQL)

target_sources(${_targetName} PRIVATE
        result.cpp
        connection.cpp
        row.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        dbprove::sql::driver
        PostgreSQL::PostgreSQL
)

