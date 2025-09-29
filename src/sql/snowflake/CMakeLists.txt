set(_targetName "dbprove_sql_snowflake")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::snowflake ALIAS ${_targetName})

target_sources(${_targetName} PRIVATE
        result.cpp
        connection.cpp
        row.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        dbprove::sql::driver
)

