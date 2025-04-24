cmake_minimum_required(VERSION 3.16)
project(mooncake-transfer-engine)

if (NOT GLOBAL_CONFIG)
  set(CMAKE_C_STANDARD 99)
  set(CMAKE_CXX_STANDARD 20)

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Wextra -Wno-unused-parameter -fPIC")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -Wall -Wextra -Wno-unused-parameter -fPIC")

  set(CMAKE_C_FLAGS_RELEASE "-O3")
  set(CMAKE_CXX_FLAGS_RELEASE "-O3")

  set(CMAKE_C_FLAGS_DEBUG "-O0")
  set(CMAKE_CXX_FLAGS_DEBUG "-O0")

  set(CMAKE_BUILD_TYPE "Release")
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

  option(BUILD_UNIT_TESTS "Build uint tests" ON)
  option(USE_ETCD "Option for enabling etcd as metadata server" OFF)
  option(USE_ETCD_LEGACY "option for enable etcd based on etcd-cpp-api-v3" OFF)
  option(USE_CUDA "option for using gpu direct" OFF)
  option(USE_NVMEOF "option for using NVMe over Fabric" OFF)
  option(USE_TCP "option for using TCP transport" ON)
  option(USE_CXL "option for using cxl protocol" OFF)
  option(USE_REDIS "option for enable redis as metadata server" OFF)
  option(USE_HTTP "option for enable http as metadata server" ON)
  option(WITH_RUST_EXAMPLE "build the Rust interface and sample code for the transfer engine" OFF)

  add_definitions(-DCONFIG_ERDMA)

  if (USE_TCP)
    add_compile_definitions(USE_TCP)
  endif()

  if (USE_CUDA)
    add_compile_definitions(USE_CUDA)
    message(STATUS "CUDA support is enabled")

    if (USE_NVMEOF)
      add_compile_definitions(USE_NVMEOF)
      message(STATUS "NVMe-oF support is enabled")
    endif()

    include_directories(/usr/local/cuda/include)
    link_directories(/usr/local/cuda/lib /usr/local/cuda/lib64)
  elseif(USE_NVMEOF)
    message(FATAL_ERROR "Cannot enable USE_NVMEOF without USE_CUDA")
  endif()

  if (USE_REDIS)
    add_compile_definitions(USE_REDIS)
    message(STATUS "Redis as metadata server support is enabled")
  endif()

  if (USE_HTTP)
    add_compile_definitions(USE_HTTP)
    message(STATUS "Http as metadata server support is enabled")
  endif()

  if (USE_ETCD)
    message(FATAL_ERROR "Cannot enable USE_ETCD while building transfer engine independently")
  endif()

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
