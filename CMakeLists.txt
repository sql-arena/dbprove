cmake_minimum_required(VERSION 3.24)
project(dbprove VERSION 0.1.0 LANGUAGES CXX)

message(STATUS "Detected compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "System name      : ${CMAKE_SYSTEM_NAME}")
message(STATUS "Triplet          : ${VCPKG_TARGET_TRIPLET}")


set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

add_compile_definitions(NOMINMAX)

include(PrintLibData)
include(cmake/EmbedSQL.cmake)

find_package(plog CONFIG REQUIRED)


if(CMAKE_GENERATOR MATCHES "Visual Studio")
    add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/MP>")
endif()

add_subdirectory(src)




