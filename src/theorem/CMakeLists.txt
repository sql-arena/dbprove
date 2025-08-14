set(_targetName "dbprove_theorem")
add_library(${_targetName} STATIC)
add_library(dbprove::theorem ALIAS ${_targetName})

target_sources(${_targetName}
        PUBLIC FILE_SET installed TYPE HEADERS BASE_DIRS include
)

target_include_directories(${_targetName} PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/dbprove/sql>)

target_sources(${_targetName}
        PRIVATE
        plan/prove.cpp
        cli/prove.cpp
        run_theorems.cpp
        runner.cpp
        types.cpp
        PRIVATE FILE_SET internal TYPE HEADERS FILES
        runner.h
        types.h
        run_theorems.h
        cli/prove.h
        plan/prove.h
)

target_embed_sql(${_targetName} SQL_FILES
        query/tpch/q01.sql
        query/tpch/q19.sql
)

target_link_libraries(${_targetName} PRIVATE
        dbprove::sql
        dbprove::generator
)

