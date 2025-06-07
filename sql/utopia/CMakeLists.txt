add_library(utopia STATIC
        connection.h
        Result.cpp
        Result.h
        Row.h
        Connection.cpp)
target_link_libraries(utopia PRIVATE sql_interface)

add_subdirectory("test")
