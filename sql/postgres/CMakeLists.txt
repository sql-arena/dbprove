find_package(PostgreSQL REQUIRED)
add_library(sql_postgres STATIC
        connection.cpp
        connection.h)
target_include_directories(sql_postgres PRIVATE
        ${CMAKE_SOURCE_DIR}/sql)
target_link_libraries(sql_postgres PUBLIC
        PostgreSQL::PostgreSQL
        sql_interface)

print_target_info(PostgreSQL::PostgreSQL)