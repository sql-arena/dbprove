find_package(CURL REQUIRED)

add_library(sql_databricks STATIC
        result.cpp
        connection.cpp
        row.cpp)
target_include_directories(sql_databricks PRIVATE
        ${CMAKE_SOURCE_DIR}/sql
        ${CURL_INCLUDE_DIRS}
)
target_link_libraries(sql_databricks PUBLIC
        sql_interface
        CURL::libcurl
)
