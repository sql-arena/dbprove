project(sql VERSION 0.1.0 LANGUAGES C CXX)

set(_targetName "dbprove_sql")
add_library(${_targetName} STATIC
        sql_exception.cpp
        explain/limit.cpp
        explain/projection.cpp
        explain/scan.cpp
        explain/scan_empty.cpp
        explain/sort.cpp
        explain/union.cpp
        explain/node.cpp
        explain/cutoff.h
        explain/join.cpp
)
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
        explain/group_by.cpp
        explain/distribution.cpp
        explain/select.cpp
        explain/scan_empty.cpp
        explain/plan.cpp
        PRIVATE FILE_SET internal TYPE HEADERS FILES
        explain/group_by.h
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

# public headers
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
)

# Make sure boilerplate compiles. These are unmapped in Engine.h
#add_subdirectory("boilerplate")

# Test it all, linking all drivers and running tests across them
if (DBPROVE_ENABLE_TESTING)
    add_subdirectory("test")
endif ()

