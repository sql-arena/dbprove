project(test_connectivity LANGUAGES CXX)
enable_testing()
add_executable(test_connectivity connection.cpp)
find_package(Catch2 CONFIG REQUIRED)
target_link_libraries(test_connectivity
        PRIVATE Catch2::Catch2WithMain
        sql
        utopia
        sql_postgres
        sql_msodbc
)
include(CTest)
include(Catch)
add_test(NAME test_connectivity COMMAND test_connectivity)
catch_discover_tests(test_connectivity)