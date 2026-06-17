# Generated compatibility export for the official prebuilt DuckDB SDK bundle.

if(NOT TARGET core_functions_extension)
    add_library(core_functions_extension STATIC IMPORTED)
    set_target_properties(core_functions_extension PROPERTIES
        INTERFACE_LINK_LIBRARIES "duckdb_static"
    )
endif()

if(NOT TARGET icu_extension)
    add_library(icu_extension STATIC IMPORTED)
    set_target_properties(icu_extension PROPERTIES
        INTERFACE_LINK_LIBRARIES "duckdb_static"
    )
endif()

if(NOT TARGET json_extension)
    add_library(json_extension STATIC IMPORTED)
    set_target_properties(json_extension PROPERTIES
        INTERFACE_LINK_LIBRARIES "duckdb_static"
    )
endif()

if(NOT TARGET parquet_extension)
    add_library(parquet_extension STATIC IMPORTED)
    set_target_properties(parquet_extension PROPERTIES
        INTERFACE_LINK_LIBRARIES "duckdb_static"
    )
endif()

if(NOT TARGET autocomplete_extension)
    add_library(autocomplete_extension STATIC IMPORTED)
    set_target_properties(autocomplete_extension PROPERTIES
        INTERFACE_LINK_LIBRARIES "duckdb_static"
    )
endif()

if(NOT TARGET jemalloc_extension)
    add_library(jemalloc_extension STATIC IMPORTED)
    set_target_properties(jemalloc_extension PROPERTIES
        INTERFACE_LINK_LIBRARIES "duckdb_static"
    )
endif()

if(NOT TARGET duckdb)
    add_library(duckdb SHARED IMPORTED)
    set_target_properties(duckdb PROPERTIES
        INTERFACE_LINK_LIBRARIES "dl;Threads::Threads"
    )
endif()

if(NOT TARGET duckdb_static)
    add_library(duckdb_static STATIC IMPORTED)
    set_target_properties(duckdb_static PROPERTIES
        INTERFACE_LINK_LIBRARIES "dl;Threads::Threads"
    )
endif()

if(NOT TARGET duckdb_generated_extension_loader)
    add_library(duckdb_generated_extension_loader STATIC IMPORTED)
endif()

include("${CMAKE_CURRENT_LIST_DIR}/DuckDBExports-release.cmake")
