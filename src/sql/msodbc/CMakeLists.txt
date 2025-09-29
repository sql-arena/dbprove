set(_targetName "dbprove_sql_msodbc")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::msodbc ALIAS ${_targetName})
find_package(ODBC REQUIRED)
find_package(azure-identity-cpp CONFIG REQUIRED)

target_sources(${_targetName}
        PRIVATE
        connection.cpp
        result.cpp
        row.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        dbprove::sql::driver
        ODBC::ODBC
        Azure::azure-identity
)

if (UNIX)
    # For Linux: we may need to link dl for dynamic loading
    target_link_libraries(AzureFabricODBC PRIVATE dl)
endif ()



