project(generator VERSION 0.1.0 LANGUAGES CXX)
set(_targetName "dbprove_generator")
add_library(${_targetName} STATIC)
add_library(dbprove::generator ALIAS ${_targetName})


target_link_libraries(${_targetName}
        PRIVATE
        dbprove::sql
        tpch/tpch_types.h
        tpch/tpch_types.cpp
        tpch/tpch_container.h
        tpch/tpch_container.cpp
        tpch/tpch_phone.cpp
        tpch/tpch_nation.h
        tpch/tpch_table_gen.cpp
        date_range.cpp
        generated_table.h
        generator_state.h
        generator_state.cpp
        tpch/tpch.h
        generated_table.cpp
)

target_link_libraries(${_targetName}
    PUBLIC
        plog::plog
        dbprove::sql
)

target_embed_sql(${_targetName} SQL_FILES
        tpch/ddl/customer.ddl
        tpch/ddl/lineitem.ddl
        tpch/ddl/nation.ddl
        tpch/ddl/orders.ddl
        tpch/ddl/part.ddl
        tpch/ddl/partsupp.ddl
        tpch/ddl/region.ddl
        tpch/ddl/supplier.ddl
)
add_subdirectory("test")
