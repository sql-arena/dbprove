find_package(duckdb REQUIRED)
add_library(sql_duckdb STATIC
        result.cpp
        connection.cpp
        connection.h
        row.cpp)
target_include_directories(sql_duckdb PRIVATE
        ${CMAKE_SOURCE_DIR}/sql)
target_link_libraries(sql_duckdb PUBLIC
        duckdb
        sql_interface)

print_target_info(duckdb)