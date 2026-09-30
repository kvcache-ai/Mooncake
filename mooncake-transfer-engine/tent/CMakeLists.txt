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

set(TENT_REPOSITORY_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../..")
execute_process(
  COMMAND git rev-parse HEAD
  WORKING_DIRECTORY "${TENT_REPOSITORY_ROOT}"
  OUTPUT_VARIABLE TENT_GIT_COMMIT
  OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
execute_process(
  COMMAND git describe --tags --always
  WORKING_DIRECTORY "${TENT_REPOSITORY_ROOT}"
  OUTPUT_VARIABLE TENT_BUILD_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
if(NOT TENT_GIT_COMMIT)
  set(TENT_GIT_COMMIT "unknown")
endif()
if(NOT TENT_BUILD_VERSION)
  set(TENT_BUILD_VERSION "unknown")
endif()

set(TENT_FEATURE_CUDA ${USE_CUDA})
set(TENT_FEATURE_HIP ${USE_HIP})
set(TENT_FEATURE_ASCEND ${USE_ASCEND})
set(TENT_FEATURE_ASCEND_DIRECT ${USE_ASCEND_DIRECT})
set(TENT_FEATURE_SUNRISE ${USE_SUNRISE})
set(TENT_FEATURE_TPU ${USE_TPU})

add_subdirectory(src)
add_subdirectory(plugins)
add_subdirectory(cli)
if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
