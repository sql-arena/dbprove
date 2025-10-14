set(_targetName "dbprove_clickhouse_sql")


add_library(${_targetName} STATIC)
add_library(dbprove::sql::clickhouse ALIAS ${_targetName})

target_sources(${_targetName}
        PRIVATE
        result.cpp
        connection.cpp
        row.cpp
        explain.cpp
)

find_library(CLICKHOUSECPP_LIB
        NAMES clickhouse-cpp-lib
        PATHS "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib"
        "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib/manual-link"
        REQUIRED)
find_path(CLICKHOUSECPP_INCLUDE_DIR
        NAMES clickhouse/client.h
        PATHS "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/include"
        REQUIRED)

add_library(clickhouse-cpp-lib UNKNOWN IMPORTED)

find_package(absl CONFIG REQUIRED)       # provides absl::int128
find_package(lz4 CONFIG REQUIRED)       # provides lz4::lz4
find_package(cityhash CONFIG REQUIRED)

set_target_properties(clickhouse-cpp-lib PROPERTIES
        IMPORTED_LOCATION "${CLICKHOUSECPP_LIB}"
        INTERFACE_INCLUDE_DIRECTORIES "${CLICKHOUSECPP_INCLUDE_DIR}"
        INTERFACE_LINK_LIBRARIES "absl::int128;lz4::lz4;cityhash"
)
add_library(clickhouse::clickhouse ALIAS clickhouse-cpp-lib)

target_link_libraries(${_targetName}
        PRIVATE
        clickhouse::clickhouse
        dbprove::sql
        dbprove::sql::driver
)