project(generator VERSION 0.1.0 LANGUAGES CXX)

add_library(generator_interface INTERFACE)
target_include_directories(generator_interface INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/generator
)
target_sources(generator_interface INTERFACE
        double_range.h
        foreign_key.h
        foreign_key.h
        generator_object.h
        Key.h
        Set.h
        tpch/tpch_text.h
        v_string.h
)
target_include_directories(generator_interface INTERFACE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/generator
)

add_library(generator STATIC tpch/tpch_text.cpp
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
        tpch/tpch.h)
target_include_directories(generator PRIVATE ${CMAKE_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/generator)
target_link_libraries(generator PUBLIC plog::plog)

target_embed_sql(generator
        tpch/ddl/customer.ddl)
add_subdirectory("test")
