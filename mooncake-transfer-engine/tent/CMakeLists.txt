cmake_minimum_required(VERSION 3.16)

if (CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(mc_tent LANGUAGES CXX)
endif()

if(BUILD_UNIT_TESTS AND NOT TARGET gtest)
    include(${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/FindGTest.cmake)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_subdirectory(src)
add_subdirectory(plugins)
if (BUILD_UNIT_TESTS)
    add_subdirectory(tests)
endif()
