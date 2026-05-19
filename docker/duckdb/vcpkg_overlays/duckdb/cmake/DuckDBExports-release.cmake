get_filename_component(_IMPORT_PREFIX "${CMAKE_CURRENT_LIST_FILE}" PATH)
get_filename_component(_IMPORT_PREFIX "${_IMPORT_PREFIX}" PATH)
get_filename_component(_IMPORT_PREFIX "${_IMPORT_PREFIX}" PATH)
if(_IMPORT_PREFIX STREQUAL "/")
    set(_IMPORT_PREFIX "")
endif()

set_property(TARGET core_functions_extension APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(core_functions_extension PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
    IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libcore_functions_extension.a"
)

set_property(TARGET icu_extension APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(icu_extension PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
    IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libicu_extension.a"
)

set_property(TARGET json_extension APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(json_extension PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
    IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libjson_extension.a"
)

set_property(TARGET parquet_extension APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(parquet_extension PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
    IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libparquet_extension.a"
)

set_property(TARGET autocomplete_extension APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(autocomplete_extension PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
    IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libautocomplete_extension.a"
)

set_property(TARGET jemalloc_extension APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(jemalloc_extension PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
    IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libjemalloc_extension.a"
)

set_property(TARGET duckdb APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(duckdb PROPERTIES
    IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libduckdb.so"
    IMPORTED_SONAME_RELEASE "libduckdb.so"
)

set_property(TARGET duckdb_static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(duckdb_static PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
    IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libduckdb_static.a"
)

set_property(TARGET duckdb_generated_extension_loader APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(duckdb_generated_extension_loader PROPERTIES
    IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "CXX"
    IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libduckdb_generated_extension_loader.a"
)
