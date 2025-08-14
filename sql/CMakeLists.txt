project(sql VERSION 0.1.0 LANGUAGES C CXX)
add_library(sql_interface INTERFACE)


find_package(cnl CONFIG REQUIRED)
add_library(cnl INTERFACE)
target_include_directories(cnl INTERFACE ${CNL_INCLUDE_DIR})

target_include_directories(sql_interface INTERFACE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/sql
)
target_sources(sql_interface INTERFACE
        connection_base.h
        result_base.h
        row_base.h
)
target_include_directories(sql_interface INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}
)
add_library(sql STATIC
        connection_factory.h
        sql_type.h
        row_iterator.cpp
        column_base.h
        result_base.cpp
        Query.h
        Query.cpp
        sql_exceptions.h
        explain/node.h
        explain/join.h
        explain/node_type.h
        explain/scan.h
        explain/group_by.h
        explain/sort.h
        explain/column.h
        explain/projection.h
        explain/distribution.h
        explain/select.h
        credential.h
        connection_base.cpp
        explain/plan.h
        explain/plan.cpp
        explain/node.cpp
        explain/group_by.cpp
        explain/union.h
        explain/selection.h
        credential.cpp
        engine.cpp
)
target_include_directories(sql PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}
)

target_link_libraries(sql PRIVATE cnl)

# Drivers
add_subdirectory("mariadb")
add_subdirectory("postgres")
target_link_libraries(sql PUBLIC sql_postgres)
add_subdirectory("utopia")
target_link_libraries(sql PUBLIC sql_utopia)
add_subdirectory("sqlite")
add_subdirectory("duckdb")
target_link_libraries(sql PUBLIC sql_duckdb)
add_subdirectory("clickhouse")
add_subdirectory("msodbc")
target_link_libraries(sql PUBLIC sql_msodbc)
add_subdirectory("databricks")
target_link_libraries(sql PUBLIC sql_databricks)


# Make sure boilerplate compiles. These are unmapped in Engine.h
add_subdirectory("boilerplate")

# Test it all, linking all drivers and running tests across them
add_subdirectory("test")

