project(sql VERSION 0.1.0 LANGUAGES C CXX)
find_package(Iconv)
find_package(pugixml CONFIG REQUIRED)

set(_coreTargetName "dbprove_sql_core")
add_library(${_coreTargetName} STATIC)
add_library(dbprove::sql::core ALIAS ${_coreTargetName})

target_sources(${_coreTargetName}
        PRIVATE
        row_iterator.cpp
        result_base.cpp
        credential.cpp
        engine.cpp
        sql_exception.cpp
        row_base.cpp
        expression.cpp
        sql_type.cpp
        parsed_table.cpp
)

target_precompile_headers(${_coreTargetName}
        PRIVATE
        <vector>
        <stdexcept>
        <string>
)

target_include_directories(${_coreTargetName}
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/dbprove/sql>
)

target_link_libraries(${_coreTargetName}
        PUBLIC
        dbprove::common
        #PRIVATE
        #Cnl::Cnl
)

add_subdirectory("explain")

set(_targetName "dbprove_sql")
add_library(${_targetName} STATIC)
add_library(dbprove::sql ALIAS ${_targetName})

target_sources(${_targetName}
        PRIVATE
        connection_base.cpp
        connection_factory.cpp
)

add_subdirectory(include/dbprove/sql)


# To implement drivers, one needs access to some raw headers that should not be visible elsewhere
add_library(sql_driver_headers INTERFACE)
add_library(dbprove::sql::driver ALIAS sql_driver_headers)
target_include_directories(sql_driver_headers
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/explain>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/dbprove/sql>)

target_embed_files(${_targetName} SQL_FILES information_schema/data_type.sql)

# Drivers
add_subdirectory("duckdb")
if (NOT DBPROVE_DUCKDB_ONLY)
    add_subdirectory("cedardb")
    add_subdirectory("mssql")
    add_subdirectory("odbc")
    add_subdirectory("postgresql")
    add_subdirectory("utopia")
    add_subdirectory("datafusion")
    add_subdirectory("databricks")
    add_subdirectory("yellowbrick")
    add_subdirectory("clickhouse")
    add_subdirectory("snowflake")
    add_subdirectory("mariadb")
    add_subdirectory("sqlite")
    add_subdirectory("trino")

    # Make sure boilerplate compiles. These are unmapped in Engine.h
    add_subdirectory("boilerplate")
endif()

target_compile_definitions(${_targetName}
        PRIVATE
        "$<$<BOOL:${DBPROVE_DUCKDB_ONLY}>:DBPROVE_DUCKDB_ONLY>"
)

target_link_libraries(${_targetName}
        PUBLIC
        dbprove::sql::core
        dbprove::sql::driver
        PRIVATE
        dbprove::sql::duckdb
)

if (NOT DBPROVE_DUCKDB_ONLY)
    target_link_libraries(${_targetName}
            PRIVATE
            dbprove::sql::cedardb
            dbprove::sql::postgres
            dbprove::sql::utopia
            dbprove::sql::datafusion
            dbprove::sql::databricks
            dbprove::sql::yellowbrick
            dbprove::sql::clickhouse
            dbprove::sql::sqlite
            dbprove::sql::snowflake
            dbprove::sql::mariadb
            dbprove::sql::trino
            dbprove::sql::mssql
    )
endif()

target_link_libraries(${_targetName}
        PRIVATE
        pugixml::pugixml
)

if (Iconv_FOUND)
    target_link_libraries(${_targetName} PRIVATE Iconv::Iconv)
endif ()

# Test it all, linking all drivers and running tests across them
if (DBPROVE_ENABLE_TESTING)
    add_subdirectory("test")
endif ()
