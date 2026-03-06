set(_targetName "dbprove_sql_mssql")

add_library(${_targetName} STATIC)
add_library(dbprove::sql::mssql ALIAS ${_targetName})
find_package(ODBC REQUIRED)
find_package(azure-identity-cpp CONFIG REQUIRED)

target_sources(${_targetName}
        PRIVATE
        connection.cpp
        explain.cpp
        bulk_load.cpp
)

find_package(absl CONFIG REQUIRED)       # provides absl::int128

target_include_directories(${_targetName}
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql::core
        dbprove::sql::driver
        dbprove::sql::odbc
        Azure::azure-identity
    )

    if (DBPROVE_ENABLE_TESTING)
        find_package(unofficial-vincentlaucsb-csv-parser CONFIG REQUIRED)
        add_executable(test_mssql_bulk_load test/test_bulk_load.cpp)
        target_link_libraries(test_mssql_bulk_load PRIVATE ${_targetName} dbprove::common unofficial::vincentlaucsb-csv-parser::csv)
        target_include_directories(test_mssql_bulk_load PRIVATE ../include)
    endif()

    if (WIN32)
        target_link_libraries(${_targetName} PRIVATE odbc32 odbcinst)
    else()
        find_package(ODBC REQUIRED)
        find_library(ODBCINST_LIB NAMES odbcinst PATHS /opt/homebrew/lib /usr/local/lib)
        if (ODBCINST_LIB)
            target_link_libraries(${_targetName} PRIVATE ODBC::ODBC ${ODBCINST_LIB})
        else()
            target_link_libraries(${_targetName} PRIVATE ODBC::ODBC odbcinst)
        endif()
    endif()


if (WIN32)
    # Try ODBC driver 18 first, then 17
    foreach (ver IN ITEMS 180 170)
        set(_inc "C:/Program Files/Microsoft SQL Server/Client SDK/ODBC/${ver}/SDK/Include")
        if (EXISTS "${_inc}/msodbcsql.h")
            target_include_directories(${_targetName} PRIVATE "${_inc}")
            message(STATUS "Found msodbcsql.h at ${_inc}")
            set(_found TRUE)
            break()
        endif ()
    endforeach ()

    if (NOT _found)
        message(WARNING "msodbcsql.h not found — install Microsoft ODBC Driver 18 SDK")
    endif ()

elseif (UNIX)
    # Try typical msodbcsql include paths on Linux/macOS
    file(GLOB _msodbc_paths "/opt/microsoft/msodbcsql*/include/msodbcsql.h" "/opt/homebrew/include/msodbcsql.h" "/usr/local/include/msodbcsql.h")
    if (_msodbc_paths)
        get_filename_component(_inc "${_msodbc_paths}" DIRECTORY)
        target_include_directories(${_targetName} PRIVATE "${_inc}")
        message(STATUS "Found msodbcsql.h at ${_inc}")
    else ()
        message(WARNING "msodbcsql.h not found — install msodbcsql-devel or fallback to odbcss.h")
        target_compile_definitions(${_targetName} PRIVATE USE_ODBCSS_H)
    endif ()
    target_link_libraries(${_targetName} PRIVATE dl)
endif ()



