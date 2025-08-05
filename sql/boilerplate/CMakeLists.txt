add_library(sql_boilerplate STATIC
        result.cpp
        connection.cpp
        connection.h
        row.cpp)
target_include_directories(sql_boilerplate PRIVATE
        ${CMAKE_SOURCE_DIR}/sql)
target_link_libraries(sql_boilerplate PUBLIC
        sql_interface)

# TODO: Find packages specific to driver and link them (public linking) to the library