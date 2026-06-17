cmake_minimum_required(VERSION 3.10)
project(cityhash CXX)

option(ENABLE_SSE "Build CityHash variants that depend on the _mm_crc32_u64 intrinsic." OFF)

include(CheckCXXCompilerFlag)

if(ENABLE_SSE)
    if(MSVC)
        set(SSE2_FLAG /arch:SSE4.2)
    else()
        check_cxx_compiler_flag(-msse4.2 HAVE_MSSE4_2)
        if(NOT HAVE_MSSE4_2)
            message(FATAL_ERROR "ENABLE_SSE requires compiler support for -msse4.2")
        endif()
        set(SSE2_FLAG -msse4.2)
    endif()
endif()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(cityhash STATIC src/city.cc)
target_include_directories(cityhash PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
    $<INSTALL_INTERFACE:include>
)

if(ENABLE_SSE)
    target_compile_options(cityhash PRIVATE ${SSE2_FLAG})
    if(NOT MSVC)
        target_compile_definitions(cityhash PRIVATE __SSE4_2__)
    endif()
endif()

install(TARGETS cityhash EXPORT cityhash-config
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)

install(FILES src/city.h DESTINATION include)
install(EXPORT cityhash-config DESTINATION share/cmake/cityhash)
