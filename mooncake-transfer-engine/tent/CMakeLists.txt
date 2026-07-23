cmake_minimum_required(VERSION 3.16)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(mc_tent LANGUAGES CXX)
endif()

if(BUILD_UNIT_TESTS AND NOT TARGET gtest)
    include(${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/FindGTest.cmake)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Compile-time metrics switch (default OFF for zero overhead on the transfer hot
# path). Declared at the top level so it is discoverable; the actual compile
# definition is applied in tent/src/metrics/CMakeLists.txt.
option(TENT_METRICS_ENABLED "Enable TENT metrics collection" OFF)

add_subdirectory(src)
add_subdirectory(plugins)
if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
