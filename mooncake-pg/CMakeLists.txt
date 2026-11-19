cmake_minimum_required(VERSION 3.16)
project(mooncake-pg)

include_directories(include)
add_subdirectory(src)
if(BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
