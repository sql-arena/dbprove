set(_targetName "dbprove_sql_databricks")
add_library(${_targetName} STATIC)
add_library(dbprove::sql::databricks ALIAS ${_targetName})
set(_vendored_duckdb_config_dir
    "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/share/duckdb")
if(EXISTS "${_vendored_duckdb_config_dir}/DuckDBConfig.cmake")
    find_package(DuckDB CONFIG REQUIRED
        PATHS "${_vendored_duckdb_config_dir}"
        NO_DEFAULT_PATH)
else()
    find_package(DuckDB CONFIG REQUIRED)
endif()

find_package(CURL REQUIRED)

target_sources(${_targetName}
        PRIVATE
        result.cpp
        connection.cpp
        row.cpp
        explain.cpp
        explain_context.cpp
        explain_context.h
)

target_include_directories(${_targetName}
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql::core
        CURL::libcurl
        duckdb_static
        duckdb_generated_extension_loader
        core_functions_extension
        parquet_extension
        dbprove::sql::driver
)
