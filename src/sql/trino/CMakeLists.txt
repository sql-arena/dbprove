set(_targetName "dbprove_sql_trino")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::trino ALIAS ${_targetName})

find_package(CURL REQUIRED)

target_sources(${_targetName} PRIVATE
        connection.cpp
        result.cpp
        row.cpp
        explain.cpp
        fix_actuals.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::generator
        dbprove::sql::core
        dbprove::sql::driver
        CURL::libcurl
)
