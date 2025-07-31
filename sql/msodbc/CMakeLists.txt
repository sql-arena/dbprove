find_package(ODBC REQUIRED)
find_package(azure-identity-cpp CONFIG REQUIRED)

add_library(sql_msodbc STATIC
        connection.cpp
        connection.h)
target_include_directories(sql_msodbc PRIVATE
        ${CMAKE_SOURCE_DIR}/sql)
target_link_libraries(sql_msodbc PUBLIC
        sql_interface
        ODBC::ODBC
        Azure::azure-identity
)



# For Linux: we may need to link dl for dynamic loading
if(UNIX)
    target_link_libraries(AzureFabricODBC PRIVATE dl)
endif()