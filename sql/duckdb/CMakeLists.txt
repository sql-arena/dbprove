find_package(duckdb REQUIRED)
add_compile_definitions(DUCKDB_STATIC_LINK)

add_library(sql_duckdb STATIC
        result.cpp
        connection.cpp
        row.cpp)
target_include_directories(sql_duckdb PRIVATE
        ${CMAKE_SOURCE_DIR}/sql)
target_link_libraries(sql_duckdb PUBLIC
        duckdb_static
        sql_interface)
target_compile_definitions(sql_duckdb PRIVATE DUCKDB_STATIC_LINK)


print_target_info(duckdb)