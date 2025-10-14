project(sql VERSION 0.1.0 LANGUAGES C CXX)

set(_targetName "dbprove_sql")
add_library(${_targetName} STATIC)
add_library(dbprove::sql ALIAS ${_targetName})
find_package(Cnl CONFIG REQUIRED)

target_sources(${_targetName}
        PUBLIC FILE_SET installed TYPE HEADERS BASE_DIRS include
)
target_sources(${_targetName}
        PRIVATE
        row_iterator.cpp
        result_base.cpp
        connection_base.cpp
        connection_factory.cpp
        credential.cpp
        engine.cpp
        sql_exception.cpp
        row_base.cpp
        expression.cpp
        sql_type.cpp
)
add_subdirectory("explain")

target_precompile_headers(${_targetName}
        PRIVATE
        <vector>
        <stdexcept>
        <string>
)

target_include_directories(${_targetName}
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/dbprove/sql>)

add_subdirectory(include/dbprove/sql)


# To implement drivers, one needs access to some raw headers that should not be visible elsewhere
add_library(sql_driver_headers INTERFACE)
add_library(dbprove::sql::driver ALIAS sql_driver_headers)
target_include_directories(sql_driver_headers
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/explain>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/dbprove/sql>)

target_embed_files(${_targetName} SQL_FILES information_schema/data_type.sql)

# Drivers

add_subdirectory("odbc")
add_subdirectory("postgresql")
add_subdirectory("utopia")
add_subdirectory("duckdb")
add_subdirectory("databricks")
add_subdirectory("msodbc")
add_subdirectory("yellowbrick")
add_subdirectory("clickhouse")
add_subdirectory("snowflake")
add_subdirectory("mariadb")
add_subdirectory("sqlite")

# Make sure boilerplate compiles. These are unmapped in Engine.h
add_subdirectory("boilerplate")

target_link_libraries(${_targetName}
        PUBLIC
        dbprove::common
        PRIVATE
        Cnl::Cnl
        dbprove::sql::postgres
        dbprove::sql::utopia
        dbprove::sql::duckdb
        dbprove::sql::databricks
        dbprove::sql::msodbc
        dbprove::sql::yellowbrick
        dbprove::sql::clickhouse
        dbprove::sql::sqlite
        dbprove::sql::snowflake
        dbprove::sql::mariadb
)


# Test it all, linking all drivers and running tests across them
if (DBPROVE_ENABLE_TESTING)
    add_subdirectory("test")
endif ()

