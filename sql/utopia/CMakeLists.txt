add_library(sql_utopia STATIC
        connection.h
        Result.cpp
        Result.h
        Row.h
        Connection.cpp)
target_link_libraries(sql_utopia PRIVATE sql_interface)

add_subdirectory("test")
