project(generator VERSION 0.1.0 LANGUAGES CXX)
set(_targetName "dbprove_generator")
add_library(${_targetName} STATIC)
add_library(dbprove::generator ALIAS ${_targetName})

target_sources(${_targetName}
        PUBLIC FILE_SET installed TYPE HEADERS BASE_DIRS include
        generator_state.h
        generated_table.h
        tpch.h
)
target_include_directories(${_targetName}
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/dbprove/generator>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

target_sources(${_targetName}
        PRIVATE
        date_range.cpp
        generator_state.cpp
        generated_table.cpp
        tpch/tpch_types.cpp
        tpch/tpch_container.cpp
        tpch/tpch_table_gen.cpp
        tpch/tpch_text.cpp
        tpch/tpch_phone.cpp
        PRIVATE FILE_SET internal TYPE HEADERS FILES
        tpch/tpch_types.h
        tpch/tpch_container.h
)
target_link_libraries(${_targetName}
        PRIVATE
        plog::plog
        PUBLIC
        dbprove::sql
)
add_subdirectory(include/dbprove/generator)
target_embed_files(${_targetName} SQL_FILES
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
