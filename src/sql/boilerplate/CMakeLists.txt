set(_targetName "dbprove_boilerplate_sql")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::boilerplate ALIAS ${_targetName})

target_sources(${_targetName}
        PRIVATE
        result.cpp
        connection.cpp
        row.cpp
        explain.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        dbprove::sql::driver
)

# TODO: Find packages specific to driver and link them (public linking) to the library