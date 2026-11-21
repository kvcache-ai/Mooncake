cmake_minimum_required(VERSION 4.0)
project(mooncake-pg)

include_directories(include)
add_subdirectory(src)
if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
