set(PREBUILT_ROOT "$ENV{DUCKDB_PREBUILT_ROOT}")
if(PREBUILT_ROOT STREQUAL "")
    message(FATAL_ERROR "DUCKDB_PREBUILT_ROOT must point at the unpacked official DuckDB SDK bundle")
endif()

set(PREBUILT_INCLUDE_DIR "${PREBUILT_ROOT}/include")
set(PREBUILT_LIB_DIR "${PREBUILT_ROOT}/lib")

foreach(required_file
    "${PREBUILT_INCLUDE_DIR}/duckdb.hpp"
    "${PREBUILT_INCLUDE_DIR}/duckdb.h"
    "${PREBUILT_INCLUDE_DIR}/duckdb_extension.h"
    "${PREBUILT_LIB_DIR}/libduckdb.so"
    "${PREBUILT_LIB_DIR}/libduckdb_static.a"
    "${PREBUILT_LIB_DIR}/libduckdb_generated_extension_loader.a"
    "${PREBUILT_LIB_DIR}/libautocomplete_extension.a"
    "${PREBUILT_LIB_DIR}/libcore_functions_extension.a"
    "${PREBUILT_LIB_DIR}/libicu_extension.a"
    "${PREBUILT_LIB_DIR}/libjson_extension.a"
    "${PREBUILT_LIB_DIR}/libparquet_extension.a"
    "${PREBUILT_LIB_DIR}/libjemalloc_extension.a"
)
    if(NOT EXISTS "${required_file}")
        message(FATAL_ERROR "Missing required DuckDB prebuilt asset: ${required_file}")
    endif()
endforeach()

file(INSTALL
    "${PREBUILT_INCLUDE_DIR}/duckdb.hpp"
    "${PREBUILT_INCLUDE_DIR}/duckdb.h"
    "${PREBUILT_INCLUDE_DIR}/duckdb_extension.h"
    DESTINATION "${CURRENT_PACKAGES_DIR}/include"
)

file(GLOB PREBUILT_LIBS
    "${PREBUILT_LIB_DIR}/*.a"
    "${PREBUILT_LIB_DIR}/*.so"
)
file(INSTALL ${PREBUILT_LIBS} DESTINATION "${CURRENT_PACKAGES_DIR}/lib")

file(INSTALL
    "${CMAKE_CURRENT_LIST_DIR}/cmake/DuckDBConfig.cmake"
    "${CMAKE_CURRENT_LIST_DIR}/cmake/DuckDBConfigVersion.cmake"
    "${CMAKE_CURRENT_LIST_DIR}/cmake/DuckDBExports.cmake"
    "${CMAKE_CURRENT_LIST_DIR}/cmake/DuckDBExports-release.cmake"
    DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}"
)

file(INSTALL "${CMAKE_CURRENT_LIST_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
file(WRITE "${CURRENT_PACKAGES_DIR}/share/${PORT}/copyright"
"DuckDB prebuilt SDK bundle assembled from the official DuckDB v1.5.0 release assets.\n")
