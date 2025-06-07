project(sql VERSION 0.1.0 LANGUAGES C CXX)
add_library(sql_interface INTERFACE)


target_include_directories(sql_interface INTERFACE
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
)
target_include_directories(sql PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}
)

# Drivers
add_subdirectory("mariadb")
add_subdirectory("postgres")
add_subdirectory("utopia")
add_subdirectory("sqlite")
add_subdirectory("duckdb")
add_subdirectory("clickhouse")
add_subdirectory("msodbc")

# Test it all, linking all drivers
add_subdirectory("test")