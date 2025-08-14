set(_targetName "dbprove_sql_utopia")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::utopia ALIAS ${_targetName})

target_sources(${_targetName}
        PRIVATE
        result.cpp
        connection.cpp
)

target_link_libraries(${_targetName}
    PUBLIC
    dbprove::sql
)

target_include_directories(${_targetName} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
add_subdirectory("test")
