set(_targetName "dbprove_ux")
add_library(${_targetName} STATIC
        boxes.cpp)
add_library(dbprove::ux ALIAS ${_targetName})

find_path(RANG_INCLUDE_DIRS "rang.hpp")
find_package(libfort CONFIG REQUIRED)

target_sources(${_targetName}
        PUBLIC FILE_SET installed TYPE HEADERS BASE_DIRS include
)

target_sources(${_targetName}
        PRIVATE
        messages.cpp
        pre_ample.cpp
        spinner.cpp
        tables.cpp
        terminal.cpp
        messages.cpp
        colour.cpp
        pretty.cpp
        PRIVATE FILE_SET internal TYPE HEADERS FILES
        glyphs.h
        boxes.h
        colour.h
)

target_link_libraries(${_targetName}
        PRIVATE
        libfort::fort
        dbprove::common
)

target_include_directories(${_targetName} PRIVATE ${RANG_INCLUDE_DIRS})

target_include_directories(${_targetName} PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/dbprove/ux>)

add_subdirectory(include/dbprove/ux)