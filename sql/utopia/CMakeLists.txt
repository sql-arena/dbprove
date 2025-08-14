add_library(sql_utopia STATIC
        connection.h
        Result.cpp
        Result.h
        Row.h
        Connection.cpp)

target_link_libraries(sql_utopia 
    PUBLIC 
        dbprove::sql
)

target_include_directories(sql_utopia PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)
add_subdirectory("test")
