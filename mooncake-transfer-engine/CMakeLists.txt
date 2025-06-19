cmake_minimum_required(VERSION 3.16)
project(mooncake-transfer-engine)

if (NOT GLOBAL_CONFIG)
  if (USE_ETCD)
    message(FATAL_ERROR "Cannot enable USE_ETCD while building transfer engine independently")
  endif()
  include(../mooncake-common/common.cmake)
endif() # GLOBAL_CONFIG

include_directories(include)
add_subdirectory(include)
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()

if (BUILD_EXAMPLES)
  add_subdirectory(example)
endif()

if (WITH_RUST_EXAMPLE)
  add_subdirectory(rust)
endif()

if (USE_MNNVL)
  add_subdirectory(nvlink-allocator)
endif()
