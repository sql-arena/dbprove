add_executable(dbreport main.cpp)

find_package(CLI11 CONFIG REQUIRED)
target_include_directories(dbreport PUBLIC ${RANG_INCLUDE_DIRS})

target_sources(dbreport
        PRIVATE
        main.cpp)

target_link_libraries(dbreport
        PRIVATE
        dbprove::ux
        CLI11::CLI11
)
