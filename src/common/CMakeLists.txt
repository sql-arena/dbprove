set(_targetName "dbprove_common")

add_library(${_targetName} STATIC)
add_library(dbprove::common ALIAS ${_targetName})

target_sources(${_targetName}
        PUBLIC FILE_SET installed TYPE HEADERS BASE_DIRS include
        PRIVATE
        docker.cpp
        pretty.cpp
        file_utility.cpp
        json_utility.cpp
        aws_bucket.cpp
)
find_package(magic_enum CONFIG REQUIRED)
find_package(CURL REQUIRED)
target_link_libraries(${_targetName}
        PUBLIC
        magic_enum::magic_enum
        CURL::libcurl)
add_subdirectory(include/dbprove/common)
