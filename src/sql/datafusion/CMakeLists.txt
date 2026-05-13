set(_targetName "dbprove_sql_datafusion")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::datafusion ALIAS ${_targetName})

target_sources(${_targetName} PRIVATE
        connection.cpp
        explain.cpp
        result.cpp
        row.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql::core
        dbprove::sql::driver
)
