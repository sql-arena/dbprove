add_executable(dbreport main.cpp)

find_package(CLI11 CONFIG REQUIRED)
find_package(pugixml CONFIG REQUIRED)
target_include_directories(dbreport PUBLIC ${RANG_INCLUDE_DIRS})

target_sources(dbreport
        PRIVATE
        main.cpp)

target_embed_files(dbreport SQL_FILES
        sql/proof.sql
        sql/engines.sql
        sql/engine_data.sql
)

target_link_libraries(dbreport
        PRIVATE
        dbprove::ux
        dbprove::sql
        CLI11::CLI11
        pugixml::pugixml
)
