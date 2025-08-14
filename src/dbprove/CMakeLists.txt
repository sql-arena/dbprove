find_package(CLI11 CONFIG REQUIRED)
add_executable(dbprove main.cpp)

target_include_directories(dbprove PUBLIC ${RANG_INCLUDE_DIRS})

target_link_libraries(dbprove
        PRIVATE
        dbprove::sql
        dbprove::generator
        dbprove::theorem
        dbprove::ux
        CLI11::CLI11
)
