project(test_utopia LANGUAGES CXX)
enable_testing()
find_package(Catch2 CONFIG REQUIRED)
add_executable(test_utopia connection.cpp
        result.cpp)

target_link_libraries(test_utopia 
    PRIVATE 
        Catch2::Catch2WithMain 
        dbprove::sql::utopia
)
#include(CTest)
#include(Catch)
add_test(NAME test_utopia COMMAND test_utopia)
