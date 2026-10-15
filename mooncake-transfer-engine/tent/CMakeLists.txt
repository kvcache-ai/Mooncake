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

# Compile-time metrics switch (default OFF for zero overhead on the transfer hot
# path). Declared at the top level so it is discoverable; the actual compile
# definition is applied in tent/src/metrics/CMakeLists.txt.
option(TENT_METRICS_ENABLED "Enable TENT metrics collection" OFF)

# Compile the MemFabric and Ascend platform sources only into isolated tests.
# The normal TENT libraries keep their CPU configuration and never use ACL
# mocks.
option(TENT_BUILD_MEMFABRIC_MOCK_TESTS
       "Build isolated MemFabric and Ascend platform tests without CANN" OFF)
if(TENT_BUILD_MEMFABRIC_MOCK_TESTS)
  if(NOT BUILD_UNIT_TESTS)
    message(
      FATAL_ERROR "TENT_BUILD_MEMFABRIC_MOCK_TESTS requires BUILD_UNIT_TESTS=ON"
    )
  endif()
  foreach(
    accelerator_option
    USE_MEMFABRIC
    USE_ASCEND
    USE_ASCEND_DIRECT
    USE_ASCEND_HETEROGENEOUS
    USE_UBSHMEM
    USE_CUDA
    USE_HIP
    USE_SUNRISE
    USE_TPU
    USE_XPU)
    if(${accelerator_option})
      message(
        FATAL_ERROR
          "TENT_BUILD_MEMFABRIC_MOCK_TESTS requires ${accelerator_option}=OFF; use a separate CPU build directory"
      )
    endif()
  endforeach()
endif()

add_subdirectory(src)
add_subdirectory(plugins)
if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
