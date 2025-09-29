set(_targetName "dbprove_sql_sqlite")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::sqlite ALIAS ${_targetName})

target_sources(${_targetName} PRIVATE
        connection.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        dbprove::sql::driver
)
