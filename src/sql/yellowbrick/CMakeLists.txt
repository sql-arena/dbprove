set(_targetName "dbprove_yellowbrick_sql")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::yellowbrick ALIAS ${_targetName})

target_sources(${_targetName}
        PRIVATE
        result.cpp
        connection.cpp
        row.cpp
)

find_package(pugixml CONFIG REQUIRED)
target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        dbprove::sql::driver
        dbprove::sql::postgres
        pugixml::pugixml
)
