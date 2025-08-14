cmake_minimum_required(VERSION 3.24)

project(dbprove VERSION 0.1.0 LANGUAGES CXX)


message(STATUS "Detected compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "System name      : ${CMAKE_SYSTEM_NAME}")
message(STATUS "Triplet          : ${VCPKG_TARGET_TRIPLET}")


set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

add_compile_definitions(dbprove NOMINMAX)
add_compile_definitions(DUCKDB_STATIC_LINK)

include(PrintLibData)
include(cmake/EmbedSQL.cmake)
find_package(CLI11 CONFIG REQUIRED)
find_package(plog CONFIG REQUIRED)
find_path(RANG_INCLUDE_DIRS "rang.hpp")


if(CMAKE_GENERATOR MATCHES "Visual Studio")
    add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/MP>")
endif()

add_subdirectory(src)

add_subdirectory(sql)
add_subdirectory(generator)
add_subdirectory(theorem)


add_executable(dbprove main.cpp
        ux/PreAmple.h
        ux/Terminal.h
        ux/Terminal.cpp
        ${THEOREM_SOURCES}
)
target_link_libraries(dbprove 
    PRIVATE 
        sql 
        generator 
        CLI11::CLI11 
        theorem
)

target_include_directories(dbprove PRIVATE ${PROJECT_SOURCE_DIR})
target_include_directories(dbprove PRIVATE ${PROJECT_SOURCE_DIR}/ux)
target_include_directories(dbprove PRIVATE ${PROJECT_SOURCE_DIR}/sql)
target_include_directories(dbprove PUBLIC ${RANG_INCLUDE_DIRS})
