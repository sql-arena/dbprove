project(test_generator LANGUAGES CXX)

enable_testing()
find_package(Catch2 CONFIG REQUIRED)
add_executable(test_generator TpcH.cpp)
target_link_libraries(test_generator PRIVATE Catch2::Catch2WithMain generator generator_interface)
add_test(NAME test_generator COMMAND test_generator )
include(CTest)
include(Catch)
catch_discover_tests(test_generator)

