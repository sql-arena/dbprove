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
        explain.cpp
)

target_include_directories(${_targetName}
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        CURL::libcurl
        dbprove::sql::driver
)


