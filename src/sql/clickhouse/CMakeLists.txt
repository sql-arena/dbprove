set(_targetName "dbprove_clickhouse_sql")


add_library(${_targetName} STATIC)
add_library(dbprove::sql::clickhouse ALIAS ${_targetName})

target_sources(${_targetName}
        PRIVATE
        result.cpp
        connection.cpp
        row.cpp
)

find_package(clickhouse-cpp CONFIG REQUIRED)
target_link_libraries(${_targetName}
        PRIVATE
        clickhouse-cpp::clickhouse-cpp
        dbprove::sql
        dbprove::sql::driver
)