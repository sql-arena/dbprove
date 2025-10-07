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
        explain.cpp
        bulk_load.cpp
)

target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        dbprove::sql::driver
        ODBC::ODBC
        Azure::azure-identity
        odbc32
)


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
    file(GLOB _msodbc_paths "/opt/microsoft/msodbcsql*/include/msodbcsql.h")
    if (_msodbc_paths)
        get_filename_component(_inc "${_msodbc_paths}" DIRECTORY)
        target_include_directories(${_targetName} PRIVATE "${_inc}")
        message(STATUS "Found msodbcsql.h at ${_inc}")
    else ()
        message(WARNING "msodbcsql.h not found — install msodbcsql-devel or fallback to odbcss.h")
        target_compile_definitions(${_targetName} PRIVATE USE_ODBCSS_H)
    endif ()
    target_link_libraries(AzureFabricODBC PRIVATE dl)
endif ()



