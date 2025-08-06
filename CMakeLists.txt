cmake_minimum_required(VERSION 3.24)

project(dbprove VERSION 0.1.0 LANGUAGES CXX)


message(STATUS "Detected compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "System name      : ${CMAKE_SYSTEM_NAME}")
message(STATUS "Triplet          : ${VCPKG_TARGET_TRIPLET}")


set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

include(PrintLibData)
include(cmake/EmbedSQL.cmake)

add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/MP>")

add_subdirectory(sql)
add_subdirectory(generator)

set(THEOREM_SOURCES
        ux/Glyph.h
        ux/Terminal.cpp
        ux/Spinner.h
        ux/Messages.h
        ux/ux.h
        runner/Runner.h
        runner/Runner.cpp
        theorem/Types.h
        theorem/cli/prove.cpp
        generator/tpch/tpch_table_gen.cpp
        common/tree_node.h
)

add_executable(dbprove main.cpp
        theorem_type.h
        theorem/cli/prove.h
        common/string.h
        ux/PreAmple.h
        ux/Terminal.h
        theorem/cli/prove.cpp
        runner/Runner.cpp
        ${THEOREM_SOURCES}
)
target_link_libraries(dbprove PRIVATE cli11)
target_link_libraries(dbprove PRIVATE rang)
target_link_libraries(dbprove PRIVATE sql)
target_link_libraries(dbprove PRIVATE generator)
target_include_directories(dbprove PRIVATE ${PROJECT_SOURCE_DIR})
target_include_directories(dbprove PRIVATE ${PROJECT_SOURCE_DIR}/ux)
target_include_directories(dbprove PRIVATE ${PROJECT_SOURCE_DIR}/sql)

