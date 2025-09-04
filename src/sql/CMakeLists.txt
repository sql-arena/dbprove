project(sql VERSION 0.1.0 LANGUAGES C CXX)

set(_targetName "dbprove_sql")
add_library(${_targetName} STATIC
        clickhouse/block_holder.h)
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
        explain/select.cpp
        explain/join.cpp
        explain/group_by.cpp
        explain/distribution.cpp
        explain/plan.cpp
        explain/limit.cpp
        explain/projection.cpp
        explain/scan.cpp
        explain/scan_empty.cpp
        explain/scan_materialised.cpp
        explain/sort.cpp
        explain/union.cpp
        explain/node.cpp
        PRIVATE FILE_SET internal TYPE HEADERS FILES
        explain/group_by.h
        explain/cutoff.h
        explain/sequence.h
)

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

# Drivers
add_subdirectory("postgres")
add_subdirectory("utopia")
add_subdirectory("duckdb")
add_subdirectory("databricks")
add_subdirectory("msodbc")
add_subdirectory("yellowbrick")
add_subdirectory("clickhouse")

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
)


# Test it all, linking all drivers and running tests across them
if (DBPROVE_ENABLE_TESTING)
    add_subdirectory("test")
endif ()

