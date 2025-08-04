find_package(duckdb REQUIRED)
add_compile_definitions(DUCKDB_STATIC_LINK)

add_library(sql_duckdb STATIC
        result.cpp
        connection.cpp
        connection.h
        row.cpp)
target_include_directories(sql_duckdb PRIVATE
        ${CMAKE_SOURCE_DIR}/sql)
target_link_libraries(sql_duckdb PUBLIC
        duckdb_static
        sql_interface)

print_target_info(duckdb)