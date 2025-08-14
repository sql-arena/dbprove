enable_testing()
find_package(Catch2 CONFIG REQUIRED)
add_executable(test_generator)

target_sources(test_generator
        PRIVATE
        tpch.cpp
        date_range.cpp)
target_link_libraries(test_generator
        PRIVATE
        Catch2::Catch2WithMain
        dbprove::generator
)
target_include_directories(test_generator PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>)
add_test(NAME test_generator COMMAND test_generator )
include(CTest)
include(Catch)
catch_discover_tests(test_generator )

