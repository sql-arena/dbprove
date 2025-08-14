set(_targetName "dbprove_sql_databricks")
add_library(${_targetName} STATIC)
add_library(dbprove::sql::databricks ALIAS ${_targetName})
find_package(duckdb REQUIRED)

find_package(CURL REQUIRED)

target_sources(${_targetName}
        PRIVATE
        result.cpp
        connection.cpp
        row.cpp
)

target_link_libraries(${_targetName} PUBLIC
        dbprove::sql
        CURL::libcurl
)
