project(test_connectivity LANGUAGES CXX)
enable_testing()
add_executable(test_connectivity
        connection.cpp
        explain.cpp
        fixture.cpp)
find_package(Catch2 CONFIG REQUIRED)

target_link_libraries(test_connectivity
        PRIVATE
        Catch2::Catch2WithMain
        dbprove::sql
        dbprove::sql::utopia
        dbprove::sql::postgres
        dbprove::sql::databricks
        dbprove::sql::clickhouse
)

target_embed_files(test_connectivity SQL_FILES
        sql/bushy_plan.sql
        sql/explain.sql
        sql/simple_join.sql
        sql/two_join.sql
        sql/union_and_join.sql
        sql/topn.sql
        sql/sql_type.sql
)

include(CTest)
include(Catch)
add_test(NAME test_connectivity COMMAND test_connectivity)
catch_discover_tests(test_connectivity)
