set(_targetName "dbprove_sql_dudkdb")
add_library(${_targetName} STATIC)
add_library(dbprove::sql::duckdb ALIAS ${_targetName})

set(DuckDB_USE_STATIC_LIBS ON)
add_compile_definitions(DUCKDB_STATIC_LINK)
set(_vendored_duckdb_config_dir
    "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/share/duckdb")
if(EXISTS "${_vendored_duckdb_config_dir}/DuckDBConfig.cmake")
    find_package(DuckDB CONFIG REQUIRED
        PATHS "${_vendored_duckdb_config_dir}"
        NO_DEFAULT_PATH)
else()
    find_package(DuckDB CONFIG REQUIRED)
endif()


target_sources(${_targetName} PRIVATE
        result.cpp
        connection.cpp
        row.cpp
        explain.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        duckdb_static
        duckdb_generated_extension_loader
        core_functions_extension
        parquet_extension
        dbprove::sql::driver
        dbprove::sql::core)


print_target_info(duckdb_static)
