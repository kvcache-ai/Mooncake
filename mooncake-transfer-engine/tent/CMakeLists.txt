cmake_minimum_required(VERSION 3.16)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(mc_tent LANGUAGES CXX)
  include(../../cmake/Dependencies.cmake)
  mooncake_provide_liburing()
  mooncake_provide_hiredis()
endif()

if(BUILD_UNIT_TESTS AND NOT TARGET gtest)
    include(${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-common/FindGTest.cmake)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL
       "Build adaptive congestion control for RDMA transports" OFF)
if(MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL)
  add_compile_definitions(MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL=1)
  if(NOT TARGET adaptive_congestion_control_core)
    add_subdirectory(../src/common/adaptive_congestion_control
                     ${CMAKE_CURRENT_BINARY_DIR}/adaptive_congestion_control)
  endif()
endif()

# Compile-time metrics switch (default OFF for zero overhead on the transfer hot
# path). Declared at the top level so it is discoverable; the actual compile
# definition is applied in tent/src/metrics/CMakeLists.txt.
option(TENT_METRICS_ENABLED "Enable TENT metrics collection" OFF)

add_subdirectory(src)
add_subdirectory(plugins)
if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
