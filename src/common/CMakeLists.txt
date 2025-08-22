set(_targetName "dbprove_common")

add_library(${_targetName} STATIC)
add_library(dbprove::common ALIAS ${_targetName})

target_sources(${_targetName}
        PUBLIC FILE_SET installed TYPE HEADERS BASE_DIRS include
        PRIVATE
        pretty.cpp
        file_utility.cpp
)
find_package(magic_enum CONFIG REQUIRED)
target_link_libraries(${_targetName}
        PUBLIC
        magic_enum::magic_enum)
add_subdirectory(include/dbprove/common)
