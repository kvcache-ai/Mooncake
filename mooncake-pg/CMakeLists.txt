cmake_minimum_required(VERSION 3.16)
project(mooncake-pg)

option(MOONCAKE_PG_HOST_TESTS_ONLY
       "Build PG host planning tests without the GPU runtime" OFF)
if(MOONCAKE_PG_HOST_TESTS_ONLY)
  enable_testing()
  add_subdirectory(tests)
  return()
endif()

include_directories(include)
add_subdirectory(src)
if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
