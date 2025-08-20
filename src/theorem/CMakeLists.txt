set(_targetName "dbprove_theorem")
add_library(${_targetName} STATIC
        init.h
        init.cpp
        run_ctx.cpp
        type.cpp
        data.cpp)
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
        PRIVATE FILE_SET internal TYPE HEADERS FILES
        runner.h
        query.h
        cli/prover.h
        plan/prover.h
)

target_embed_sql(${_targetName} SQL_FILES
        query/tpch/q01.sql
        query/tpch/q19.sql
)

target_link_libraries(${_targetName} PRIVATE
        dbprove::sql
        dbprove::generator
        dbprove::ux
        dbprove::common
)

# TODO: Integrate CSV writer here