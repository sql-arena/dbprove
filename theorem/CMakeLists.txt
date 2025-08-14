add_library(theorem STATIC
        plan/prove.h
        plan/prove.cpp
        cli/prove.h
        cli/prove.cpp
        run_theorems.h
        run_theorems.cpp
        runner.cpp
        runner.h
        types.h
        types.cpp
)

target_embed_sql(theorem SQL_FILES
        query/tpch/q01.sql
        query/tpch/q19.sql
)

target_link_libraries(theorem
    PRIVATE
        dbprove::sql
)

target_include_directories(theorem PRIVATE ${CMAKE_SOURCE_DIR})
target_include_directories(theorem PUBLIC ${RANG_INCLUDE_DIRS})
