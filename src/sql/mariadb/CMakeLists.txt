set(_targetName "dbprove_sql_mariadb")

find_package(unofficial-libmariadb CONFIG REQUIRED)

add_library(${_targetName} STATIC)
add_library(dbprove::sql::mariadb ALIAS ${_targetName})

target_sources(${_targetName} PRIVATE
        connection.cpp
        result.cpp
        row.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        dbprove::sql::driver
)
