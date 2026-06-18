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
        generator_state.cpp
        generated_table.cpp
)
find_package(google_cloud_cpp_storage CONFIG REQUIRED)
find_package(libzip CONFIG REQUIRED)

target_link_libraries(${_targetName}
        PRIVATE
        plog::plog
        google-cloud-cpp::storage
        libzip::zip
        dbprove::ux
        PUBLIC
        dbprove::sql::core
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
        job/ddl/aka_name.sql
        job/ddl/aka_title.sql
        job/ddl/cast_info.sql
        job/ddl/char_name.sql
        job/ddl/comp_cast_type.sql
        job/ddl/company_name.sql
        job/ddl/company_type.sql
        job/ddl/complete_cast.sql
        job/ddl/info_type.sql
        job/ddl/keyword.sql
        job/ddl/kind_type.sql
        job/ddl/link_type.sql
        job/ddl/movie_companies.sql
        job/ddl/movie_info.sql
        job/ddl/movie_info_idx.sql
        job/ddl/movie_keyword.sql
        job/ddl/movie_link.sql
        job/ddl/name.sql
        job/ddl/person_info.sql
        job/ddl/role_type.sql
        job/ddl/title.sql
)
