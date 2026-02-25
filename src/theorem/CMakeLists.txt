set(_targetName "dbprove_theorem")
add_library(${_targetName} STATIC)
add_library(dbprove::theorem ALIAS ${_targetName})

target_sources(${_targetName}
        PUBLIC FILE_SET installed TYPE HEADERS BASE_DIRS include
)

target_include_directories(${_targetName} PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/dbprove/theorem>)

target_sources(${_targetName}
        PRIVATE
        plan/prove.cpp
        cli/prove.cpp
        runner.cpp
        proof.cpp
        prover.cpp
        query.cpp
        init.cpp
        run_ctx.cpp
        type.cpp
        data.cpp
        test_theorem.cpp
        PRIVATE FILE_SET internal TYPE HEADERS FILES
        runner.h
        query.h
        init.h
        cli/prover.h
        plan/prover.h
)

target_embed_files(${_targetName} SQL_FILES
        query/tpch/q01.sql
        query/tpch/q02.sql
        query/tpch/q03.sql
        query/tpch/q04.sql
        query/tpch/q05.sql
        query/tpch/q06.sql
        query/tpch/q07.sql
        query/tpch/q08.sql
        query/tpch/q09.sql
        query/tpch/q10.sql
        query/tpch/q11.sql
        query/tpch/q12.sql
        query/tpch/q13.sql
        query/tpch/q14.sql
        query/tpch/q15.sql
        query/tpch/q16.sql
        query/tpch/q17.sql
        query/tpch/q18.sql
        query/tpch/q19.sql
        query/tpch/q20.sql
        query/tpch/q21.sql
        query/tpch/q22.sql
)

find_package(unofficial-vincentlaucsb-csv-parser CONFIG REQUIRED)
find_package(magic_enum CONFIG REQUIRED)

target_link_libraries(${_targetName} PRIVATE
        dbprove::sql
        dbprove::generator
        dbprove::ux
        dbprove::common
        unofficial::vincentlaucsb-csv-parser::csv
        PUBLIC
        magic_enum::magic_enum
)

# TODO: Integrate CSV writer here