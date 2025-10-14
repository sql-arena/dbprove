set(_targetName "dbprove_sql_odbc")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::odbc ALIAS ${_targetName})
find_package(ODBC REQUIRED)

target_sources(${_targetName}
        PRIVATE
        connection.cpp
        result.cpp
        row.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        dbprove::sql::driver
        ODBC::ODBC
)
