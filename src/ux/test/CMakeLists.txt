project(test_ux LANGUAGES CXX)
enable_testing()
find_package(Catch2 CONFIG REQUIRED)

add_executable(test_ux terminal.cpp)

target_link_libraries(test_ux
    PRIVATE
    Catch2::Catch2WithMain
    dbprove::ux
)

add_test(NAME test_ux COMMAND test_ux)
include(CTest)
include(Catch)
catch_discover_tests(test_ux)
