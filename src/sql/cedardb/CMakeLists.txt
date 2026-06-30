set(_targetName "dbprove_sql_cedardb")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::cedardb ALIAS ${_targetName})

target_sources(${_targetName} PRIVATE
        connection.cpp
        explain.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql::postgres   # provides postgresql::Connection base class + libpq
        dbprove::sql::core
        dbprove::sql::driver
)
